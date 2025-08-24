/* ===== 0) 匯入所有檔案 ===== */
proc import out=list
     datafile="/home/u64061874/list.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=avg_PQ_final2_sorted
     datafile="/home/u64061874/avg_PQ_announce_sorted.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=financial_data
     datafile="/home/u64061874/financial_data.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=GDP_growth
     datafile="/home/u64061874/GDP_growth.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=patent_announce
     datafile="/home/u64061874/patent_announce.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ===== 1) 統一 PQ 表欄位名（apply_year→year、id→companyID） ===== */
data avg_PQ_final2_sorted2;
    set avg_PQ_final2_sorted;
    year = apply_year;
    companyID = id;
run;

/* ===== 2) 建立「落後一年」的 PQ 表（2004 用 2003 的 PQ） ===== */
proc sort data=avg_PQ_final2_sorted2 nodupkey; 
    by companyID year; 
run;

data avg_PQ_lag;
  set avg_PQ_final2_sorted2;
  year = year + 1;               /* 整體往後移一年 */
  rename avg_PQ = avg_PQ_lag;
run;

/* ===== 3) 合併（含所有財務欄位與新版專利欄位） ===== */
proc sql;
    create table merged_data as
    select 
        a.*,
        bl.avg_PQ_lag,

        /* financial_data 全欄位（之後會轉數值） */
        c.ROE, c.Equity, c.TA, c.ROA, c.ARTR,
        c.labor_cost, c.interest_income, c.interest_cost,
        c.brokerage_income, c.loan_income, c.lending_income,
        c.underwriting_income, c.stock_income, c.dividend_income,
        c.futures_income, c.securities_income, c.settlement_income,
        c.futures_management_income, c.management_fee_income, c.consulting_fee_income,
        c.margin_loans_receivable, c.current_ratio,

        d.GDP_growth,

        /* 新版專利欄位（允許缺失，下一步補 0） */
        e.invention_acc, e.new_acc, e.design_acc, e.total_acc,
        e.invention_sin, e.new_sin, e.design_sin
    from list as a
    left join avg_PQ_lag as bl
        on a.year = bl.year and a.companyID = bl.companyID
    left join financial_data as c
        on a.year = c.year and a.companyID = c.companyID
    left join GDP_growth as d
        on a.year = d.year
    left join patent_announce as e
        on a.year = e.year and a.companyID = e.companyID
    ;
quit;

/* ===== 5) 專利欄缺失補 0（*_acc 與 *_sin）===== */
data merged_data_filled;
    set merged_data;  /* 來自第 3 步的 merged_data */
    array pat_acc[4] invention_acc new_acc design_acc total_acc;
    array pat_sin[3] invention_sin new_sin design_sin;
    do _i = 1 to dim(pat_acc); if missing(pat_acc[_i]) then pat_acc[_i] = 0; end;
    do _j = 1 to dim(pat_sin); if missing(pat_sin[_j]) then pat_sin[_j] = 0; end;
    drop _i _j;
run;

/* ===== 6) 公司名稱合併（保留所有原值，不刪除列）===== */
proc sort data=list out=company_map(keep=companyID companyName) nodupkey; 
  by companyID; 
run;

data merged_with_key;
  set merged_data_filled;
  length keyID $40;
  keyID = cats(companyID);
run;

data company_map_key;
  set company_map;
  length keyID $40;
  keyID = cats(companyID);
  keep keyID companyName;
run;

proc sql;
  create table merged_with_cname_raw as
  select 
      m.*,
      coalescec(m.companyName, c.companyName) as companyName_fix length=256
  from merged_with_key as m
  left join company_map_key as c
    on m.keyID = c.keyID;
quit;

data merged_with_cname;
  set merged_with_cname_raw;
  drop companyName;
  rename companyName_fix = companyName;
run;

/* ===== 7) 產出最終資料集（移除 keyID；其餘原值全保留）===== */
data patent_regression_data1;
  set merged_with_cname(drop=keyID);
run;

/* ===== 8)（移除）不做任何轉數值或清洗 ===== */
/* （此步留白） */

/* ===== 9) 排序並依指定欄位順序輸出（僅調整欄位順序，值不變）===== */
proc sort data=patent_regression_data1 
          out=patent_regression_data1_ann;
  by year companyID;
run;

/* 只重排欄位位置：把你關心的放前面，其他保持原值照樣保留 */
proc sql noprint;
  select name into :other_cols separated by ' '
  from dictionary.columns
  where libname='WORK' 
    and memname='PATENT_REGRESSION_DATA1_ANN'
    and upcase(name) not in (
      'YEAR','COMPANYID','COMPANYNAME',
      'INVENTION_ACC','NEW_ACC','DESIGN_ACC','TOTAL_ACC',
      'INVENTION_SIN','NEW_SIN','DESIGN_SIN','ROE'
    );
quit;

data patent_regression_data1_ann;
  retain year companyID companyName 
         invention_acc new_acc design_acc total_acc
         invention_sin new_sin design_sin ROE &other_cols;
  set patent_regression_data1_ann;  /* 只重排欄位順序，不改變任何值 */
run;

/* ===== 10) 匯出 Excel（所有欄位原樣保留）===== */
proc export data=patent_regression_data1_ann
    outfile="/home/u64061874/patent_regression_data1_announce.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data1_announce";
    putnames=yes;
run;