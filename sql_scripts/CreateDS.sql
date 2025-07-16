create schema if not exists ds;

create table ds.ft_balance_f (
	on_date date not null,
	account_rk numeric not null,
	currency_rk numeric,
    balance_out double precision
);

create table ds.ft_posting_f (
	oper_date date not null,
    credit_account_rk numeric not null,
    debet_account_rk numeric not null,
    credit_amount double precision,
    debet_amount double precision
);

create table ds.md_account_d (
	data_actual_date date not null,
    data_actual_end_date date not null,
    account_rk numeric not null,
    account_number varchar(20) not null,
    char_type varchar(1) not null,
    currency_rk numeric not null,
    currency_code varchar(3) not null
);

create table ds.md_currency_d (
	currency_rk numeric not null,
    data_actual_date date not null,
    data_actual_end_date date,
    currency_code varchar(3),
    code_iso_char varchar(3)
);

create table ds.md_exchange_rate_d (
	data_actual_date date not null,
    data_actual_end_date date,
    currency_rk numeric not null,
    reduced_cource double precision,
    code_iso_num varchar(3)
);

create table ds.md_ledger_account_s (
	chapter char(1),
    chapter_name varchar(16),
    section_number integer,
    section_name varchar(22),
    subsection_name varchar(21),
    ledger1_account integer,
    ledger1_account_name varchar(47),
    ledger_account integer not null,
    ledger_account_name varchar(153),
    characteristic char(1),
    is_resident integer,
    is_reserve integer,
    is_reserved integer,
    is_loan integer,
    is_reserved_assets integer,
    is_overdue integer,
    is_interest integer,
    pair_account varchar(5),
    start_date date not null,
    end_date date,
    is_rub_only integer,
    min_term varchar(1),
    min_term_measure varchar(1),
    max_term varchar(1),
    max_term_measure varchar(1),
    ledger_acc_full_name_translit varchar(1),
    is_revaluation varchar(1),
    is_correct varchar(1)
);



alter table ds.ft_balance_f add primary key (on_date, account_rk);
alter table ds.md_account_d add primary key (data_actual_date, account_rk);
alter table ds.md_currency_d add primary key (currency_rk, data_actual_date);
alter table ds.md_exchange_rate_d add primary key (data_actual_date, currency_rk);
alter table ds.md_ledger_account_s add primary key (ledger_account, start_date);



SELECT 
    c.column_name
FROM 
    information_schema.table_constraints tc 
    JOIN information_schema.constraint_column_usage AS ccu 
        ON ccu.constraint_name = tc.constraint_name
    JOIN information_schema.columns AS c 
        ON c.table_schema = tc.table_schema
        AND c.table_name = tc.table_name
        AND c.column_name = ccu.column_name
WHERE 
    tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema = 'ds'
    AND tc.table_name = 'md_account_d';



