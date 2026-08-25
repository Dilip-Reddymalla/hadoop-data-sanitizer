package com.bda.sanitizer;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Filters messy HR employee CSV records.
 *
 * A record is emitted only if it passes every validation rule. Rules are
 * evaluated in a fixed order and the FIRST failure classifies the record, so
 * each rejected row increments exactly one reason counter. That invariant makes
 * the counters reconcile exactly:
 *
 *   INVALID_RECORDS = MALFORMED_RECORDS
 *                   + BLANK_FIELD_RECORDS
 *                   + INVALID_ID_FORMAT_RECORDS
 *                   + INVALID_DATE_RECORDS
 *                   + INVALID_CATEGORY_RECORDS
 *                   + INVALID_NUMERIC_RECORDS
 *                   + LOGICAL_INCONSISTENCY_RECORDS
 *
 * The rules escalate from cheapest and most structural to most semantic:
 * shape -> presence -> identity -> time -> vocabulary -> magnitude -> logic.
 *
 * Output key   = Employee_ID  (lets the reducer collapse duplicate IDs)
 * Output value = the normalised, RFC-4180-quoted record
 */
public class DataSanitizerMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    /** Counter group name, referenced by the driver when printing the report. */
    public static final String GROUP = "DATA_SANITIZATION";

    public enum Counters {
        TOTAL_RECORDS,
        VALID_RECORDS,
        INVALID_RECORDS,
        MALFORMED_RECORDS,
        BLANK_FIELD_RECORDS,
        INVALID_ID_FORMAT_RECORDS,
        INVALID_DATE_RECORDS,
        INVALID_CATEGORY_RECORDS,
        INVALID_NUMERIC_RECORDS,
        LOGICAL_INCONSISTENCY_RECORDS,
        QUOTED_FIELD_RECORDS,
        HEADER_ROWS_SKIPPED
    }

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    private SimpleDateFormat[] dateParsers;
    private SimpleDateFormat isoOut;
    private Calendar calendar;

    @Override
    protected void setup(Context context) {
        dateParsers = new SimpleDateFormat[RecordSchema.DATE_FORMATS.length];
        for (int i = 0; i < RecordSchema.DATE_FORMATS.length; i++) {
            SimpleDateFormat f =
                new SimpleDateFormat(RecordSchema.DATE_FORMATS[i], Locale.US);
            // Reject things like month 13 or day 32 instead of rolling over.
            f.setLenient(false);
            dateParsers[i] = f;
        }
        isoOut = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
        calendar = Calendar.getInstance(Locale.US);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip the CSV header wherever it appears (once per input split file).
        if (line.startsWith("Employee_ID,")) {
            context.getCounter(GROUP, Counters.HEADER_ROWS_SKIPPED.name())
                   .increment(1);
            return;
        }
        if (line.trim().isEmpty()) {
            // A blank line is structurally malformed, not a data problem.
            context.getCounter(GROUP, Counters.TOTAL_RECORDS.name()).increment(1);
            reject(context, Counters.MALFORMED_RECORDS);
            return;
        }

        context.getCounter(GROUP, Counters.TOTAL_RECORDS.name()).increment(1);

        // Quote-aware split. A naive line.split(",") would tear apart the 436
        // rows whose Full_Name is quoted because it contains a comma, e.g.
        //   EMP0005335,"Sil Verboom-Werl-Arnsberg, van",IT,...
        // and misfile 436 perfectly good employees as malformed.
        String[] f = splitCsv(line);
        if (line.indexOf('"') >= 0) {
            context.getCounter(GROUP, Counters.QUOTED_FIELD_RECORDS.name())
                   .increment(1);
        }

        // RULE 1 - structural: exact column count.
        if (f.length != RecordSchema.EXPECTED_COLUMNS) {
            reject(context, Counters.MALFORMED_RECORDS);
            return;
        }

        // RULE 2 - every required field present and not a placeholder token.
        for (int idx : RecordSchema.REQUIRED_FIELDS) {
            if (RecordSchema.isBlank(f[idx])) {
                reject(context, Counters.BLANK_FIELD_RECORDS);
                return;
            }
        }

        // RULE 3 - Employee_ID must look like EMP followed by 7 digits.
        String id = f[RecordSchema.IDX_EMPLOYEE_ID].trim();
        if (!isValidId(id)) {
            reject(context, Counters.INVALID_ID_FORMAT_RECORDS);
            return;
        }

        // RULE 4 - Hire_Date must be a real calendar date in range, and its
        // year must agree with the separate Year column (cross-field check).
        Date hired = parseDate(f[RecordSchema.IDX_HIRE_DATE].trim());
        if (hired == null) {
            reject(context, Counters.INVALID_DATE_RECORDS);
            return;
        }
        calendar.setTime(hired);
        int hireYear = calendar.get(Calendar.YEAR);
        if (!String.valueOf(hireYear).equals(f[RecordSchema.IDX_YEAR].trim())) {
            reject(context, Counters.INVALID_DATE_RECORDS);
            return;
        }

        // RULE 5 - categorical fields must come from their closed vocabulary.
        for (RecordSchema.CategoryRule c : RecordSchema.CATEGORY_RULES) {
            if (!c.allowed.contains(f[c.index].trim())) {
                reject(context, Counters.INVALID_CATEGORY_RECORDS);
                return;
            }
        }

        // RULE 6 - numeric fields must parse and sit inside their range.
        for (RecordSchema.NumericRule r : RecordSchema.NUMERIC_RULES) {
            String raw = f[r.index];
            if (RecordSchema.isBlank(raw)) {
                if (r.mandatory) {
                    reject(context, Counters.INVALID_NUMERIC_RECORDS);
                    return;
                }
                continue;   // optional and absent -> acceptable
            }
            if (!isNumericInRange(raw.trim(), r)) {
                reject(context, Counters.INVALID_NUMERIC_RECORDS);
                return;
            }
        }

        // RULE 7 - cross-field logic: a career cannot be longer than a life.
        double age = Double.parseDouble(f[RecordSchema.IDX_AGE].trim());
        double exp = Double.parseDouble(f[RecordSchema.IDX_EXPERIENCE].trim());
        if (age < exp + RecordSchema.MIN_ENTRY_AGE) {
            reject(context, Counters.LOGICAL_INCONSISTENCY_RECORDS);
            return;
        }

        // Survived every rule: normalise and emit, keyed by Employee_ID.
        context.getCounter(GROUP, Counters.VALID_RECORDS.name()).increment(1);
        outKey.set(id);
        outValue.set(normalise(f, hired));
        context.write(outKey, outValue);
    }

    private void reject(Context context, Counters reason) {
        context.getCounter(GROUP, Counters.INVALID_RECORDS.name()).increment(1);
        context.getCounter(GROUP, reason.name()).increment(1);
    }

    // ------------------------------------------------------------ CSV parsing ---
    /**
     * Minimal RFC-4180 field splitter: honours double-quoted fields, embedded
     * commas and the "" escape for a literal quote. Trailing empty fields are
     * preserved so the column count stays meaningful.
     */
    static String[] splitCsv(String line) {
        List<String> out = new ArrayList<>(RecordSchema.EXPECTED_COLUMNS);
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');   // "" -> literal quote
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out.toArray(new String[0]);
    }

    /** Re-quotes any field that contains a comma or a quote, then joins. */
    static String joinCsv(String[] fields) {
        StringBuilder sb = new StringBuilder(256);
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            String v = fields[i];
            if (v.indexOf(',') >= 0 || v.indexOf('"') >= 0) {
                sb.append('"').append(v.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(v);
            }
        }
        return sb.toString();
    }

    // -------------------------------------------------------------- validators ---
    private static boolean isValidId(String id) {
        if (id.length() != 10 || !id.startsWith("EMP")) {
            return false;
        }
        for (int i = 3; i < 10; i++) {
            if (!Character.isDigit(id.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** Returns the parsed date, or null if no accepted format matches. */
    private Date parseDate(String raw) {
        for (SimpleDateFormat f : dateParsers) {
            ParsePosition pos = new ParsePosition(0);
            Date d = f.parse(raw, pos);
            // Require the whole string to be consumed, else "2020-13-01" style
            // partial matches would slip through.
            if (d != null && pos.getIndex() == raw.length()) {
                calendar.setTime(d);
                int year = calendar.get(Calendar.YEAR);
                if (year < RecordSchema.MIN_YEAR || year > RecordSchema.MAX_YEAR) {
                    return null;
                }
                return d;
            }
        }
        return null;
    }

    private static boolean isNumericInRange(String raw, RecordSchema.NumericRule r) {
        double d;
        try {
            d = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            return false;
        }
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return false;
        }
        if (r.integerOnly && d != Math.floor(d)) {
            return false;
        }
        return d >= r.min && d <= r.max;
    }

    // ------------------------------------------------------------ normalisation ---
    /**
     * Light, loss-free normalisation of records that already passed validation:
     * trim every field, canonicalise the hire date, title-case the free-text
     * geography, and cast the whole-number columns out of float notation
     * ("92992.0" -> "92992") so downstream tools read them as integers.
     */
    private String normalise(String[] f, Date hired) {
        String[] out = new String[f.length];
        for (int i = 0; i < f.length; i++) {
            out[i] = f[i].trim().replaceAll("\\s+", " ");
        }

        out[RecordSchema.IDX_HIRE_DATE] = isoOut.format(hired);

        // Department / Job_Level / Status / Work_Mode / Performance_Rating are
        // closed vocabularies already validated by RULE 5 - nothing to fix.
        out[RecordSchema.IDX_COUNTRY]    = titleCase(out[RecordSchema.IDX_COUNTRY]);
        out[RecordSchema.IDX_CITY]       = titleCase(out[RecordSchema.IDX_CITY]);
        out[RecordSchema.IDX_JOB_TITLE]  = titleCase(out[RecordSchema.IDX_JOB_TITLE]);

        out[RecordSchema.IDX_SALARY]     = asInteger(out[RecordSchema.IDX_SALARY]);
        out[RecordSchema.IDX_EXPERIENCE] = asInteger(out[RecordSchema.IDX_EXPERIENCE]);
        out[RecordSchema.IDX_AGE]        = asInteger(out[RecordSchema.IDX_AGE]);
        out[RecordSchema.IDX_YEAR]       = asInteger(out[RecordSchema.IDX_YEAR]);

        return joinCsv(out);
    }

    /** "92992.0" -> "92992"; leaves anything unparseable untouched. */
    private static String asInteger(String s) {
        try {
            double d = Double.parseDouble(s);
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return String.valueOf((long) d);
            }
        } catch (NumberFormatException e) {
            // fall through - validation already passed, so keep the raw text
        }
        return s;
    }

    private static String titleCase(String s) {
        if (s.isEmpty()) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        boolean up = true;
        for (char c : s.toCharArray()) {
            sb.append(up ? Character.toUpperCase(c) : Character.toLowerCase(c));
            up = (c == ' ' || c == '&' || c == '-');
        }
        return sb.toString();
    }
}
