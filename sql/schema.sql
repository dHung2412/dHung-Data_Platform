-- DEV ENV
DROP TABLE IF EXISTS Dim_Customer CASCADE;
DROP TABLE IF EXISTS Dim_Date CASCADE;
DROP TABLE IF EXISTS Dim_Card CASCADE;
DROP TABLE IF EXISTS Dim_Account CASCADE;
DROP TABLE IF EXISTS Dim_Branch CASCADE;
DROP TABLE IF EXISTS Dim_Loan CASCADE;

DROP TABLE IF EXISTS Fact_Transaction CASCADE;
DROP TABLE IF EXISTS Fact_Loan CASCADE;
DROP TABLE IF EXISTS Fact_Feedback CASCADE;
DROP TABLE IF EXISTS Fact_Card CASCADE;

-----------------------------
-----------Dimension---------
-----------------------------
-- 1. Dim Customer
CREATE TABLE Dim_Customer (
    customer_key                SERIAL PRIMARY KEY,       -- Surrogate Key
    customer_id_source          VARCHAR(50) NOT NULL,
    birth_year                  INT,
    gender                      VARCHAR(50) NOT NULL,
    city                        VARCHAR(100) NOT NULL,

    -- SCD Type 2
    valid_from_date DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT unique_dim_customer_id_current UNIQUE (customer_id_source, valid_to_date)
);
COMMENT ON TABLE Dim_Customer IS "Lưu trữ thông tin của khách hàng (SCD Type 2).";

CREATE TABLE Dim_Customer_PII (
    customer_key                INT PRIMARY KEY REFERENCES Dim_Customer(customer_key) ON DELETE CASCADE,
    first_name                  VARCHAR(100) NOT NULL,
    last_name                   VARCHAR(100) NOT NULL,
    address                     VARCHAR(200) NOT NULL,
    contact_number              VARCHAR(50) NOT NULL,
    email                       VARCHAR(50) NOT NULL,
);
COMMENT ON TABLE Dim_Customer_PII IS "Lưu trữ thông tin chi tiết của khách hàng.";

CREATE INDEX idx_dim_customer_business_key ON Dim_Customer(customer_id_source); -- Index

-- 2. Dim Date
CREATE TABLE Dim_Date (
    date_key                    DATE PRIMARY KEY,        -- YYYY-MM-DD       -- Semantic Key
    full_date_desc              VARCHAR(20) NOT NULL,    -- DD-MM-YYYY
    day_of_week_num             SMALLINT NOT NULL,       -- Chủ nhật = 1, ...
    day_of_week_name            VARCHAR(10) NOT NULL,    -- 'Thứ hai', ...
    day_of_month                SMALLINT NOT NULL,       -- Ngày '1' -> Ngày '31'
    month_num                   SMALLINT NOT NULL,       -- Tháng '1' -> Tháng '12'
    month_name                  ARCHAR(20) NOT NULL      -- 'Tháng 1'
    quarter_num                 SMALLINT NOT NULL,       -- Quý '1' -> '4'
    year_num                    SMALLINT NOT NULL,       -- '2025'
    is_weekend                  BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday                  BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE Dim_Date IS 'Lưu trữ các thuộc tính về thời gian.';

-- 3. Dim Loan
CREATE TABLE Dim_Loan (
    loan_key                    SERIAL PRIMARY KEY,       -- Surrogate Key
    loan_id_source              VARCHAR(50) NOT NULL,     -- Bussiness Key
    loan_type                   VARCHAR(100) NOT NULL,
    loan_amount                 NUMERIC(18,2) NOT NULL,
    interest_rate               NUMERIC(5,4) NOT NULL,    -- Lãi suất (vd: 0.0700)
    loan_term                   INT NOT NULL,             -- Thời hạn vay
    current_loan_status         VARCHAR(20) NOT NULL,     -- Trạng thái cuối cùng 

    -- SCD Type 2
    valid_from_date DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to_date DATE DEFAULT '9999-12-31'
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT unique_dim_loan_id_current UNIQUE (loan_id_source, valid_to_date)
);
COMMENT ON TABLE Dim_Loan IS 'Lưu trữ các thuộc tính của hợp đồng vay (SCD Type 2).';
CREATE INDEX idx_dim_loan_bussiness_key ON Dim_Loan(loan_id_source);

-- 4. Dim Branch
CREATE TABLE Dim_Branch (
    branch_key                  SERIAL PRIMARY KEY,
    branch_id_source            VARCHAR(50) NOT NULL UNIQUE, -- Bussiness Key
    branch_name                 VARCHAR(255),  
    branch_location             VARCHAR(255)             
);
COMMENT ON TABLE Dim_Branch IS 'Lưu trữ thông tin chi nhánh.';

-- 5. Dim Account
CREATE TABLE Dim_Account(
    account_key                 SERIAL PRIMARY KEY,
    account_id_source           VARCHAR(50) NOT NULL,       -- Rebuild
    account_type                VARCHAR(100) NOT NULL,
    date_of_account_opening     DATE NOT NULL,    
    last_transaction_date       DATE,                       -- Upload by Source -> overwrite by ETL

    -- SCD Type 2
    valid_from_date DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT unique_dim_account_id_current UNIQUE (account_id, valid_to_date)
);
COMMENT ON TABLE Dim_Account IS 'Lưu trữ thông tin các tài khoản ngân hàng (SCD Type 2).';
CREATE INDEX idx_dim_account_business_key ON Dim_Account(account_id);

-- 6. Dim Card
CREATE TABLE Dim_Card (
    card_key                    SERIAL PRIMARY KEY,
    card_id_source              VARCHAR(50) NOT NULL,       -- Rebuild
    card_type                   VARCHAR(50) NOT NULL,       -- Rebuild,
    credit_limit                NUMERIC(18,2) NOT NULL,      -- Hạn mức tín dụng
    rewards_points              INT DEFAULT 0,              -- Số điểm thưởng hiện tại -> overwrite by ETL
    
    -- SCD Type 2
    valid_from_date DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    CONSTRAINT unique_dim_card_id_current UNIQUE (card_id_source, valid_to_date)
);
COMMENT ON TABLE Dim_Card IS 'Lưu trữ các thuộc tính của thẻ tín dụng/ghi nợ (SCD Type 2).';
CREATE INDEX idx_dim_card_business_key ON Dim_Card(card_id_source);

-----------------------------
-------------Fact------------
-----------------------------
-- 1. Fact Transaction
CREATE TABLE Fact_Transaction (
    transaction_key                 SERIAL PRIMARY KEY,
    transaction_id_source           VARCHAR(100) NOT NULL,      -- Degenerate Key

    -- FK
    date_key                        DATE NOT NULL,              -- From Transaction Date
    customer_key                    INT NOT NULL,
    account_key                     INT NOT NULL,
    branch_key                      INT NOT NULL,
    card_key                        INT,

    -- Degenerate Dimensions
    transaction_type                VARCHAR(50) NOT NULL,
    anomaly_flag                    TEXT,

    -- Measure
    transaction_amount              NUMERIC(18,2) NOT NULL,
    acc_balancer_after_transaction  NUMERIC(18,2) NOT NULL,     -- Semi-additive

    -- FK Constraint
    CONSTRAINT fk_fact_trans_date FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_trans_customer FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
    CONSTRAINT fk_fact_trans_account FOREIGN KEY (account_key) REFERENCES Dim_Account(account_key),
    CONSTRAINT fk_fact_trans_branch FOREIGN KEY (branch_key) REFERENCES Dim_Branch(branch_key),
    Constraint fk_fact_trans_card FOREIGN KEY (card_key) REFERENCES Dim_Card(card_key)
);
COMMENT ON TABLE Fact_Transaction IS 'Ghi lại chi tiết mỗi giao dịch. Granularity: 1 hàng / 1 giao dịch.';
CREATE INDEX idx_fact_trans_date_key ON Fact_Transaction(date_key);
CREATE INDEX idx_fact_trans_customer_key ON Fact_Transaction(customer_key);
CREATE INDEX idx_fact_trans_account_key ON Fact_Transaction(account_key);
CREATE INDEX idx_fact_trans_branch_key ON Fact_Transaction(branch_key);
CREATE INDEX idx_fact_trans_card_key ON Fact_Transaction(card_key);

-- 2. Fact Account Snapshot
CREATE TABLE Fact_Account_Snapshot (
    snapshot_date_key               DATE NOT NULL,
    account_key                     INT NOT NULL,
    customer_key                    INT NOT NULL,

    -- Measure
    account_balace                  NUMERIC(18,2) NOT NULL,

    PRIMARY KEY (snapshot_date_key, account_key),
    CONSTRAINT fk_fact_acc_snap_date FOREIGN KEY (snapshot_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_acc_snap_account FOREIGN KEY (account_key) REFERENCES Dim_Account(account_key),
    CONSTRAINT fk_fact_acc_snap_customer FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key)
);
COMMENT ON TABLE Fact_Account_Snapshot IS 'Lưu snapshot số dư tài khoản cuối (mỗi ngày).';

-- 3. Fact Card Snapshot
CREATE TABLE Fact_Card_Snapshot (
    snapshot_date_key               DATE NOT NULL,
    card_key                        INT NOT NULL,
    customer_key                    INT NOT NULL,

    -- Measure
    credit_card_balance             NUMERIC(18,2) NOT NULL,
    minimum_payment_due             NUMERIC(18,2) NOT NULL,

    -- Degenerate Dimensions
    payment_due_date                DATE,

    PRIMARY KEY (snapshot_date_key, card_key),
    CONSTRAINT fk_fact_card_snap_date FOREIGN KEY (snapshot_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_card_snap_card FOREIGN KEY (card_key) REFERENCES Dim_Date(card_key),
    CONSTRAINT fk_fact_card_snap_customer FOREIGN KEY (customer_key) REFERENCES Dim_Date(customer_key)
);
COMMENT ON TABLE Fact_Card_Snapshot IS 'Lưu snapshot tình trạng thẻ (ví dụ: vào ngày sao kê).';

-- 4. Fact Loan Application
CREATE TABLE Fact_Loan_Application (
    application_key                 SERIAL PRIMARY KEY,

    -- FK
    application_date_key            DATE NOT NULL, -- from "Approval/Rejection Date"
    customer_key                    INT NOT NULL,
    loan_key                        INT NOT NULL,

    -- Degenerate Dimensions
    application_status              VARCHAR(50) NOT NULL, -- from "Loan status"

    -- FK Constraint
    CONSTRAINT fk_fact_loan_app_date FOREIGN KEY (application_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_loan_app_customer FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
    CONSTRAINT fk_fact_loan_app_loan FOREIGN KEY (loan_key) REFERENCES Dim_Loan(loan_key)
);
COMMENT ON TABLE Fact_Loan_Application IS 'Ghi lại sự kiện một khoản vay được duyệt hoặc từ chối.';
CREATE INDEX idx_fact_loan_app_date_key ON Fact_Loan_Application(application_date_key);
CREATE INDEX idx_fact_loan_app_customer_key ON Fact_Loan_Application(customer_key);
CREATE INDEX idx_fact_loan_app_loan_key ON Fact_Loan_Application(loan_key);

-- 5. Fact Feedback
CREATE TABLE Fact_Feedback (
    feedback_key                    SERIAL PRIMARY KEY,
    feedback_id                     VARCHAR(50) NOT NULL,

    -- FK
    feedback_date_key               DATE NOT NULL, -- from "Feedback Date"
    resolution_date_key             DATE, -- from "Resolution Date"
    customer_key                    INT NOT NULL,

    -- Degenerate Dimensions
    feedback_type                   VARCHAR(200),
    resolution_status               VARCHAR(50),

    -- FK Constraint
    CONSTRAINT fk_fact_feedback_date FOREIGN KEY (feedback_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_feedback_res_date FOREIGN KEY (resolution_date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT fk_fact_feedback_customer FOREIGN KEY (customer_key) REFERENCES Dim_Customer(customer_key),
);
COMMENT ON TABLE Fact_Feedback IS 'Ghi lại các sự kiện phản hồi từ khách hàng.';
CREATE INDEX idx_fact_feedback_date_key ON Fact_Feedback(feedback_date_key);
CREATE INDEX idx_fact_feedback_res_date_key ON Fact_Feedback(feedbackresolution_date_key_date_key);
CREATE INDEX idx_fact_feedback_customer_key ON Fact_Feedback(customer_key);
