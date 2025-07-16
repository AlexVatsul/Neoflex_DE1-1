TRUNCATE TABLE ds.ft_balance_f;
TRUNCATE TABLE ds.ft_posting_f;
TRUNCATE TABLE ds.md_account_d;
TRUNCATE TABLE ds.md_currency_d;
TRUNCATE TABLE ds.md_exchange_rate_d;
TRUNCATE TABLE ds.md_ledger_account_s;


select *
from ds.ft_balance_f fbf;

select count(*)
from ds.ft_balance_f fbf;

select *
from ds.ft_posting_f;

select count(*)
from ds.ft_posting_f;

select *
from ds.md_account_d;

select count(*)
from ds.md_account_d;


select *
from ds.md_currency_d;

select count(*)
from ds.md_currency_d;


select *
from ds.md_exchange_rate_d;

select count(*)
from ds.md_exchange_rate_d;


select *
from ds.md_ledger_account_s;


select count(*)
from ds.md_ledger_account_s;


select *
from ds.ft_balance_f fbf;

select count(*)
from ds.ft_balance_f fbf;



SELECT * FROM DS.ft_balance_f 
WHERE account_rk = '13631';





