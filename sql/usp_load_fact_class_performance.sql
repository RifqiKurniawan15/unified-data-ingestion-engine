-- PROCEDURE: gold.usp_load_fact_class_performance()

-- DROP PROCEDURE IF EXISTS gold.usp_load_fact_class_performance();

CREATE OR REPLACE PROCEDURE gold.usp_load_fact_class_performance(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_error_msg TEXT;
    v_error_detail TEXT;
BEGIN
    -- 1. Ensure the Gold schema and table exist
    CREATE SCHEMA IF NOT EXISTS gold;

    CREATE TABLE IF NOT EXISTS gold.fact_class_performance (
        class_id VARCHAR(50),
        performance_date DATE,
        total_students INT,
        students_with_attendance INT,
        present_count INT,
        absent_count INT,
        attendance_rate DECIMAL(5,2),
        students_with_assessment INT,
        assessment_count INT,
        avg_score DECIMAL(5,2),
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. Clean old data for the current date or full refresh
    TRUNCATE TABLE gold.fact_class_performance;

    -- 3. Transformation: Aggregate data from Silver Layer
    INSERT INTO gold.fact_class_performance (
        class_id, performance_date, total_students, 
        students_with_attendance, present_count, absent_count, attendance_rate,
        students_with_assessment, assessment_count, avg_score
    )
    WITH attendance_agg AS (
        SELECT 
            s.class_id,
            a.attendance_date,
            COUNT(DISTINCT a.student_id) as students_with_attendance,
            COUNT(CASE WHEN a.status = 'Present' THEN 1 END) as present_count,
            COUNT(CASE WHEN a.status = 'Absent' THEN 1 END) as absent_count
        FROM silver.students s
        JOIN silver.attendance a ON s.student_id = a.student_id
        GROUP BY 1, 2
    ),
    assessment_agg AS (
        SELECT 
            s.class_id,
            asmt.assessment_date,
            COUNT(DISTINCT asmt.student_id) as students_with_assessment,
            COUNT(asmt.assessment_id) as assessment_count,
            AVG(asmt.score) as avg_score
        FROM silver.students s
        JOIN silver.assessments asmt ON s.student_id = asmt.student_id
        GROUP BY 1, 2
    ),
    class_total AS (
        SELECT class_id, COUNT(student_id) as total_students
        FROM silver.students
        GROUP BY 1
    )
    SELECT 
        ct.class_id,
        COALESCE(att.attendance_date, ass.assessment_date) as performance_date,
        ct.total_students,
        COALESCE(att.students_with_attendance, 0),
        COALESCE(att.present_count, 0),
        COALESCE(att.absent_count, 0),
        CASE 
            WHEN ct.total_students > 0 THEN ROUND(COALESCE(att.present_count, 0)::DECIMAL / ct.total_students, 2)
            ELSE 0 
        END as attendance_rate,
        COALESCE(ass.students_with_assessment, 0),
        COALESCE(ass.assessment_count, 0),
        ROUND(COALESCE(ass.avg_score, 0), 2)
    FROM class_total ct
    LEFT JOIN attendance_agg att ON ct.class_id = att.class_id
    LEFT JOIN assessment_agg ass ON ct.class_id = ass.class_id AND att.attendance_date = ass.assessment_date;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_msg = MESSAGE_TEXT,
                          v_error_detail = PG_EXCEPTION_DETAIL;

    INSERT INTO silver.error_logs (proc_name, error_message, error_detail)
    VALUES ('usp_load_fact_class_performance', v_error_msg, v_error_detail);
    
    RAISE EXCEPTION 'Gold Layer Process Failed: %', v_error_msg;
END;
$BODY$;

ALTER PROCEDURE gold.usp_load_fact_class_performance()
    OWNER TO root;

