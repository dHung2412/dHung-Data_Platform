CREATE SCHEMA IF NOT EXISTS risk_dwh;

-- Schema mặc định cho phiên làm việc
SET search_path = risk_dwh;


/*********************************************************************
 * DIMENSION TABLES (BẢNG CHIỀU)
 * Các bảng này lưu trữ "who", "what", "where", "when", "why"
 *********************************************************************/
 
-- Bảng dim thời gian 
-- Bảng này thường được điền dữ liệu bằng một thủ tục (procedure)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_date (
    date_key          INTEGER PRIMARY KEY,
    full_date         DATE NOT NULL,
    day_of_week       SMALLINT NOT NULL, -- (1=Chủ Nhật, 7=Thứ 7)
    day_of_month      SMALLINT NOT NULL,
    day_of_year       SMALLINT NOT NULL,
    week_of_year      SMALLINT NOT NULL,
    month             SMALLINT NOT NULL,
    month_name        VARCHAR(20) NOT NULL,
    quarter           SMALLINT NOT NULL,
    year              SMALLINT NOT NULL,
    is_weekend        BOOLEAN NOT NULL,
    is_holiday        BOOLEAN NOT NULL DEFAULT FALSE
);

-- Bảng dim danh mục rủi ro (Risk category)
-- Thường có cấu trúc phân cấp (Hierarchy)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_risk_category (
    risk_category_key   INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    risk_category_code  VARCHAR(20) UNIQUE,  -- Business key
    risk_category_l1    VARCHAR(100) NOT NULL, -- Cấp 1 (ví dụ: Rủi ro Tín dụng, Rủi ro Vận hành)
    risk_category_l2    VARCHAR(100),          -- Cấp 2 (ví dụ: Rủi ro Gian lận)
    risk_category_l3    VARCHAR(100),          -- Cấp 3 (ví dụ: Gian lận nội bộ)
    description         TEXT
);

-- Bảng dim đơn vị kinh doanh
-- Thường có cấu trúc phân cấp (Hierarchy)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_business_unit (
    business_unit_key   INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    business_unit_code  VARCHAR(20) UNIQUE, -- Business key
    unit_name           VARCHAR(150) NOT NULL,
    department_name     VARCHAR(100),
    division_name       VARCHAR(100),
    region_name         VARCHAR(100)
);

-- Bảng dim biện pháp kiểm soát (Control)
-- Mô tả các biện pháp được áp dụng để giảm thiểu rủi ro
CREATE TABLE IF NOT EXISTS risk_dwh.dim_control (
    control_key         INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    control_id          VARCHAR(50) UNIQUE NOT NULL, -- Business key
    control_name        VARCHAR(255) NOT NULL,
    control_description TEXT,
    control_type        VARCHAR(50), -- (ví dụ: 'Preventive', 'Detective', 'Corrective')
    control_owner       VARCHAR(100),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE
);

-- Bảng dim khách hàng 
-- Thiết kế theo dạng SCF Type 2 (Slowly Changing Dimension)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_customer (
    customer_key        BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, -- Surrogate key
    customer_id         VARCHAR(50) NOT NULL,  -- Business key
    customer_name       VARCHAR(255),
    customer_segment    VARCHAR(100),
    industry            VARCHAR(100),
    region              VARCHAR(100),
    -- Cột cho SCD Type 2
    valid_from          TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to            TIMESTAMP,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Index cho business key và cột cờ is_current
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON risk_dwh.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON risk_dwh.dim_customer(is_current);

-- Bảng dim sản phẩm (Product)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_product (
    product_key         INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id          VARCHAR(50) NOT NULL UNIQUE, -- Business key
    product_name        VARCHAR(255) NOT NULL,
    product_category    VARCHAR(100),
    product_line        VARCHAR(100)
);

-- Bảng dim mức độ rủi ro (Risk Rating)
-- (ví dụ: Thấp, Trung bình, Cao, Rất cao)
CREATE TABLE IF NOT EXISTS risk_dwh.dim_risk_rating (
    risk_rating_key     INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    risk_rating_label   VARCHAR(50) NOT NULL, -- (ví dụ: 'Low', 'Medium', 'High', 'Critical')
    rating_value_min    DECIMAL(5, 2), -- Điểm tối thiểu
    rating_value_max    DECIMAL(5, 2)  -- Điểm tối đa
);

/*********************************************************************
 * FACT TABLES (BẢNG SỰ KIỆN)
 * Các bảng này lưu trữ các số liệu (lớn)
 *********************************************************************/

-- Bảng SỰ KIỆN RỦI RO ĐÃ XẢY RA (Risk Events)
-- Lưu trữ các sự kiện rủi ro đã thực sự xảy ra (ví dụ: một giao dịch gian lận)
CREATE TABLE IF NOT EXISTS risk_dwh.fact_risk_event (
    event_pk            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,     -- Khóa chính của bảng fact
    event_id            VARCHAR(50) NOT NULL,      -- Business key từ hệ thống nguồn
    
    -- --- Foreign Keys (Khóa ngoại) ---
    event_date_key      INTEGER NOT NULL REFERENCES risk_dwh.dim_date(date_key),
    discovery_date_key  INTEGER NOT NULL REFERENCES risk_dwh.dim_date(date_key),
    risk_category_key   INTEGER NOT NULL REFERENCES risk_dwh.dim_risk_category(risk_category_key),
    business_unit_key   INTEGER NOT NULL REFERENCES risk_dwh.dim_business_unit(business_unit_key),
    control_key         INTEGER REFERENCES risk_dwh.dim_control(control_key), -- Biện pháp kiểm soát (nếu có)
    customer_key        BIGINT REFERENCES risk_dwh.dim_customer(customer_key),
    product_key         INTEGER REFERENCES risk_dwh.dim_product(product_key),

    -- --- Measures (Số liệu) ---
    gross_loss_amount   DECIMAL(18, 2) NOT NULL DEFAULT 0.00, -- Tổn thất ban đầu
    recovery_amount     DECIMAL(18, 2) NOT NULL DEFAULT 0.00, -- Số tiền thu hồi được
    net_loss_amount     DECIMAL(18, 2) NOT NULL DEFAULT 0.00, -- Tổn thất ròng
    event_count         INTEGER NOT NULL DEFAULT 1 -- Thường dùng để đếm
);

-- Tạo Index trên các khóa ngoại của bảng Fact để tăng tốc độ JOIN
CREATE INDEX IF NOT EXISTS idx_fact_risk_event_date ON risk_dwh.fact_risk_event(event_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_event_risk ON risk_dwh.fact_risk_event(risk_category_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_event_unit ON risk_dwh.fact_risk_event(business_unit_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_event_cust ON risk_dwh.fact_risk_event(customer_key);

-- Bảng SỰ KIỆN ĐÁNH GIÁ RỦI RO (Risk Assessments)
-- Lưu trữ các snapshot (bản ghi) của việc đánh giá rủi ro theo thời gian
-- (ví dụ: hàng tháng đánh giá rủi ro tín dụng của khách hàng)
CREATE TABLE IF NOT EXISTS risk_dwh.fact_risk_assessment (
    assessment_pk       BIGSERIAL PRIMARY KEY,
    assessment_id       VARCHAR(50) NOT NULL, -- Business key
    
    -- --- Foreign Keys (Khóa ngoại) ---
    assessment_date_key INTEGER NOT NULL REFERENCES risk_dwh.dim_date(date_key),
    customer_key        BIGINT REFERENCES risk_dwh.dim_customer(customer_key),
    product_key         INTEGER REFERENCES risk_dwh.dim_product(product_key),
    business_unit_key   INTEGER REFERENCES risk_dwh.dim_business_unit(business_unit_key),
    risk_category_key   INTEGER NOT NULL REFERENCES risk_dwh.dim_risk_category(risk_category_key),
    risk_rating_key     INTEGER REFERENCES risk_dwh.dim_risk_rating(risk_rating_key),

    -- --- Measures (Số liệu) ---
    risk_score          DECIMAL(10, 4), -- Điểm rủi ro (ví dụ: 0-1000)
    probability         DECIMAL(5, 4),  -- Xác suất xảy ra (ví dụ: 0.0 - 1.0)
    impact_amount       DECIMAL(18, 2), -- Mức độ ảnh hưởng (nếu tính ra tiền)
    expected_loss       DECIMAL(18, 2)  -- Tổn thất dự kiến (Probability * Impact)
);

-- Tạo Index trên các khóa ngoại
CREATE INDEX IF NOT EXISTS idx_fact_risk_assess_date ON risk_dwh.fact_risk_assessment(assessment_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_assess_cust ON risk_dwh.fact_risk_assessment(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk_assess_risk ON risk_dwh.fact_risk_assessment(risk_category_key);