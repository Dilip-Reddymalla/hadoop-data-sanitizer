package com.bda.sanitizer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Single source of truth for the HR employee CSV contract: column layout,
 * placeholder tokens, required fields, categorical domains, numeric ranges and
 * the cross-field invariant.
 *
 * Every constant below was derived by profiling the actual Kaggle files
 * (hr_raw.csv, 2,000,000 rows x 15 columns) rather than from the dataset
 * README, which is wrong in two places:
 *
 *   1. It claims the age/experience invariant is  Age = Experience + 22.
 *      The publisher's own cleaned file never goes below  Age = Experience + 21
 *      (exact minimum over all 2,000,000 cleaned rows), so 21 is the real
 *      floor. Using 22 would reject 164,794 rows instead of 48,356.
 *   2. It claims "Hierarchical Errors: Junior employees with higher salaries
 *      than Directors". Profiling shows Junior salaries top out at 70,787 and
 *      Director salaries start at 144,600 - the ranges do not overlap, so that
 *      defect class does not exist in this file and is NOT checked here.
 */
public final class RecordSchema {

    private RecordSchema() { }

    /** Exact column count every well-formed record must have. */
    public static final int EXPECTED_COLUMNS = 15;

    public static final String[] COLUMNS = {
        "Employee_ID", "Full_Name", "Department", "Job_Title", "Hire_Date",
        "Performance_Rating", "Experience_Years", "Status", "Work_Mode",
        "Salary", "Year", "Country", "City", "Age", "Job_Level"
    };

    // Column indices used by the validator / normaliser.
    public static final int IDX_EMPLOYEE_ID  = 0;
    public static final int IDX_FULL_NAME    = 1;
    public static final int IDX_DEPARTMENT   = 2;
    public static final int IDX_JOB_TITLE    = 3;
    public static final int IDX_HIRE_DATE    = 4;
    public static final int IDX_PERFORMANCE  = 5;
    public static final int IDX_EXPERIENCE   = 6;
    public static final int IDX_STATUS       = 7;
    public static final int IDX_WORK_MODE    = 8;
    public static final int IDX_SALARY       = 9;
    public static final int IDX_YEAR         = 10;
    public static final int IDX_COUNTRY      = 11;
    public static final int IDX_CITY         = 12;
    public static final int IDX_AGE          = 13;
    public static final int IDX_JOB_LEVEL    = 14;

    /**
     * Every column is required: an HR record with a hole in it is not
     * analytically usable. In the raw file only Performance_Rating is ever
     * blank, but the rule is stated over the whole schema so a future file with
     * different holes is still caught.
     */
    public static final int[] REQUIRED_FIELDS = {
        IDX_EMPLOYEE_ID, IDX_FULL_NAME, IDX_DEPARTMENT, IDX_JOB_TITLE,
        IDX_HIRE_DATE, IDX_PERFORMANCE, IDX_EXPERIENCE, IDX_STATUS,
        IDX_WORK_MODE, IDX_SALARY, IDX_YEAR, IDX_COUNTRY, IDX_CITY,
        IDX_AGE, IDX_JOB_LEVEL
    };

    /** Tokens that count as "blank" even though the cell is not empty. */
    public static final Set<String> PLACEHOLDERS = new HashSet<>(Arrays.asList(
        "", "n/a", "na", "null", "none", "unknown", "-", "--", "?", "nan", "<na>"
    ));

    // ------------------------------------------------------ categorical sets ---
    /** A categorical column plus the closed set of values it may take. */
    public static final class CategoryRule {
        public final int index;
        public final String name;
        public final Set<String> allowed;

        CategoryRule(int index, String name, String... allowed) {
            this.index = index;
            this.name = name;
            this.allowed = new HashSet<>(Arrays.asList(allowed));
        }
    }

    public static final List<CategoryRule> CATEGORY_RULES = Arrays.asList(
        new CategoryRule(IDX_DEPARTMENT, "Department",
                "Sales", "IT", "Operations", "Finance", "HR"),
        new CategoryRule(IDX_JOB_LEVEL, "Job_Level",
                "Junior", "Mid", "Senior", "Director"),
        new CategoryRule(IDX_STATUS, "Status",
                "Active", "Resigned", "Terminated", "Retired"),
        new CategoryRule(IDX_WORK_MODE, "Work_Mode",
                "On-site", "Remote", "Hybrid"),
        new CategoryRule(IDX_PERFORMANCE, "Performance_Rating",
                "Excellent", "Good", "Satisfactory", "Needs Improvement")
    );

    // ---------------------------------------------------------- numeric rules ---
    /** A numeric column plus its accepted inclusive range. */
    public static final class NumericRule {
        public final int index;
        public final String name;
        public final double min;
        public final double max;
        public final boolean integerOnly;
        /** true => must also be non-blank (blank is a validation failure). */
        public final boolean mandatory;

        NumericRule(int index, String name, double min, double max,
                    boolean integerOnly, boolean mandatory) {
            this.index = index;
            this.name = name;
            this.min = min;
            this.max = max;
            this.integerOnly = integerOnly;
            this.mandatory = mandatory;
        }
    }

    public static final List<NumericRule> NUMERIC_RULES = Arrays.asList(
        // Salary must be strictly positive; 3,333 raw rows carry a negative wage.
        new NumericRule(IDX_SALARY,     "Salary",           0.01, 1.0e7, false, true),
        new NumericRule(IDX_EXPERIENCE, "Experience_Years", 0.0,   60.0, true,  true),
        new NumericRule(IDX_AGE,        "Age",             16.0,   75.0, true,  true),
        new NumericRule(IDX_YEAR,       "Year",          1990.0, 2026.0, true,  true)
    );

    /** Hire_Date layouts, tried in order. The raw file is uniformly ISO. */
    public static final String[] DATE_FORMATS = {
        "yyyy-MM-dd",
        "dd-MM-yyyy",
        "MM/dd/yyyy",
        "yyyy/MM/dd"
    };

    /** Plausible hire-year window; anything outside is rejected. */
    public static final int MIN_YEAR = 1990;
    public static final int MAX_YEAR = 2026;

    /**
     * Cross-field invariant:  Age >= Experience_Years + MIN_ENTRY_AGE.
     *
     * Nobody can have accumulated more career years than their life allows. The
     * floor of 21 is not a guess: it is the exact minimum of (Age - Experience)
     * across all 2,000,000 rows of the publisher's own cleaned file.
     */
    public static final int MIN_ENTRY_AGE = 21;

    /** Canonical output layout for a sanitised record. */
    public static final String CLEAN_HEADER = String.join(",", COLUMNS);

    public static boolean isBlank(String v) {
        return v == null || PLACEHOLDERS.contains(v.trim().toLowerCase());
    }
}
