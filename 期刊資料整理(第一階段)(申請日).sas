/* ===== 0) 匯入所有檔案 ===== */
proc import out=list
     datafile="/home/u64061874/list.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=avg_PQ_final2_sorted
     datafile="/home/u64061874/avg_PQ_final2_sorted.xlsx"
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

proc import out=patent_apply
     datafile="/home/u64061874/patent_apply.xlsx"
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
    left join patent_apply as e
        on a.year = e.year and a.companyID = e.companyID
    ;
quit;

/* ===== 4) 允許缺失：avg_PQ_lag 與所有專利欄可缺，其餘不可缺 ===== */
data merged_data_nomiss;
    set merged_data;
    if ( cmiss(of _all_) 
         - cmiss(of avg_PQ_lag 
                   invention_acc new_acc design_acc total_acc
                   invention_sin new_sin design_sin) ) = 0;
run;

/* ===== 5) 專利欄缺失補 0（*_acc 與 *_sin）===== */
data merged_data_filled;
    set merged_data_nomiss;
    array pat_acc[4] invention_acc new_acc design_acc total_acc;
    array pat_sin[3] invention_sin new_sin design_sin;
    do _i = 1 to dim(pat_acc); if missing(pat_acc[_i]) then pat_acc[_i] = 0; end;
    do _j = 1 to dim(pat_sin); if missing(pat_sin[_j]) then pat_sin[_j] = 0; end;
    drop _i _j;
run;

/* ===== 6) 刪除任一財務欄「只有 '-'」的列（保留負數）===== */
%let fin_cols = ROE Equity TA ROA ARTR
                labor_cost interest_income interest_cost
                brokerage_income loan_income lending_income
                underwriting_income stock_income dividend_income
                futures_income securities_income settlement_income
                futures_management_income management_fee_income consulting_fee_income
                margin_loans_receivable current_ratio;

data merged_data_nohyphen;
    set merged_data_filled;
    length _var $64 _txt _clean $400;

    do _k = 1 to countw("&fin_cols");
        _var = scan("&fin_cols", _k);
        _txt   = vvaluex(_var);
        _txt   = ktranslate(_txt, '-', '–—－');  /* 全形/破折→半形 '-' */
        _txt   = kstrip(_txt);                   /* 去前後空白（含全形） */
        _clean = kcompress(_txt, , 's');         /* 去所有空白類 */
        if _clean = '-' then delete;
    end;

    drop _k _var _txt _clean;
run;

/* ===== 7) 加上公司中文名稱（用 keyID 合併，避免型別不合）===== */
proc sort data=list out=company_map(keep=companyID companyName) nodupkey; 
  by companyID; 
run;

/* 建字串鍵以避免型別不合（cats 可同時處理字元/數值） */
data merged_with_key;
  set merged_data_nohyphen;
  length keyID $40;
  keyID = cats(companyID);
run;

data company_map_key;
  set company_map;
  length keyID $40;
  keyID = cats(companyID);
  keep keyID companyName;
run;

/* 先產生不重名欄位 companyName_fix，再覆蓋 */
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

/* ===== 8) 產出最終資料集（移除 keyID）===== */
data patent_regression_data1;
  set merged_with_cname(drop=keyID);
run;

/* ===== 9) 將所有財務欄位轉成數值（穩健清洗版）===== */
%macro to_numeric(ds=, out=, vars=);
data &out;
  set &ds;
  %local i v n;
  %let n=%sysfunc(countw(&vars));
  %do i=1 %to &n;
    %let v=%scan(&vars,&i);
    /* 若本來就是數值，直接複製；否則清洗後轉換 */
    if vtypex("&v")='N' then &v._num = &v;
    else do;
      length __s $400;
      __s = vvaluex("&v");                  /* 取顯示值 */
      __s = ktranslate(__s, ' ', 'A0'x);    /* NBSP -> space */
      __s = ktranslate(__s, ',', '，');     /* 全形逗號 -> 半形 */
      __s = kstrip(__s);
      if prxmatch('/^\(.*\)$/', __s) then __s = cats('-', substr(__s,2,length(__s)-2)); /* (1,234)→-1234 */
      __s = compress(__s, ', ');            /* 去千分位逗號與空白 */
      if not missing(__s) and char(__s,length(__s))='%' then do;
         __s = substr(__s,1,length(__s)-1);
         &v._num = input(__s, best32.)/100;
      end;
      else &v._num = input(__s, best32.);
      drop __s;
    end;
    drop &v; rename &v._num=&v;
  %end;
run;
%mend;

/* 轉數值（如要一併處理 GDP_growth，將其加到 &fin_cols） */
%to_numeric(ds=patent_regression_data1,
           out=patent_regression_data1_num,
           vars=&fin_cols);

/* ===== 10) 排序並依指定欄位順序輸出 ===== */

/* 10-1 排序 year companyID，並用最終簡短名稱 */
proc sort data=patent_regression_data1_num 
          out=patent_regression_data1_app;
  by year companyID;
run;

/* 10-2 抓取「其他欄位」清單，排除你想放前面的欄位 */
proc sql noprint;
    select name into :other_cols separated by ' '
    from dictionary.columns
    where libname='WORK' 
          and memname='PATENT_REGRESSION_DATA1_APP'  /* 注意：系統字典中為大寫 */
          and upcase(name) not in (
            'YEAR','COMPANYID','COMPANYNAME',
            'INVENTION_ACC','NEW_ACC','DESIGN_ACC','TOTAL_ACC',
            'INVENTION_SIN','NEW_SIN','DESIGN_SIN','ROE'
          );
quit;

/* 10-3 重新建立資料集，欄位順序：指定欄位 + 其他欄位 */
data patent_regression_data1_app;
    retain year companyID companyName 
           invention_acc new_acc design_acc total_acc
           invention_sin new_sin design_sin ROE &other_cols;
    set patent_regression_data1_app;
run;

/* 10-4 匯出到 Excel（檔名/工作表改為 *_apply / *_apply） */
proc export data=patent_regression_data1_app
    outfile="/home/u64061874/patent_regression_data1_apply.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data1_apply";
    putnames=yes;
run;