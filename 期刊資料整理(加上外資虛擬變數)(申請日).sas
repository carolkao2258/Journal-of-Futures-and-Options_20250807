/* ========= 路徑 ========= */
%let merge_xlsx  = /home/u64061874/merge.xlsx;
%let prd3_xlsx   = /home/u64061874/patent_regression_data3_apply.xlsx;   /* 來源：data3_app */
%let foreign_xlsx= /home/u64061874/foreign_company_apply.xlsx;           /* 中繼輸出 */
%let out5_xlsx   = /home/u64061874/patent_regression_data5_apply.xlsx;   /* 最終輸出 */

/* 小工具：計數 *
%macro chk(ds);
  %local nobs;
  %if %sysfunc(exist(&ds)) %then %do;
    proc sql noprint; select count(*) into :nobs from &ds; quit;
    %put NOTE: &=ds has &nobs rows.;
  %end;
  %else %put NOTE: &=ds does not exist.;
%mend;

/* 正規化 companyID／公司名：全半形→半形、去空白、轉大寫、純數字補 Z6. */
%macro norm_company(ds=, out=, idvar=companyID, namevar=companyName);
data &out;
  set &ds;
  length &idvar._key $32 name_key $200 __raw $64;

  /* 名稱鍵（給名稱對應用） */
  name_key = upcase(compbl(strip(&namevar)));

  /* ID 鍵（給 ID 對應／join 用） */
  __raw = strip(vvalue(&idvar));
  __raw = translate(__raw
           ,'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
           ,'０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ');
  __raw = compress(__raw, ' '||'　'||byte(160));
  __raw = upcase(__raw);
  if prxmatch('/^[0-9]+$/', __raw) then &idvar._key = put(input(__raw, ?? best32.), z6.);
  else &idvar._key = __raw;

  drop __raw &idvar;
  rename &idvar._key=&idvar;
run;
%mend;

/* ========= 1) 匯入兩份檔案 ========= */
proc import datafile="&merge_xlsx"
    out=merge_raw dbms=xlsx replace;
    getnames=yes;
run;

proc import datafile="&prd3_xlsx"
    out=patent_raw dbms=xlsx replace;
    sheet="data3_app";          /* 與你前段輸出的 sheet 對齊 */
    getnames=yes;
run;

/* ========= 2) 正規化公司名稱／ID ========= */
/* merge.xlsx：只有名稱（欄位名你原先寫 securuties，沿用） */
data merge_n;
  set merge_raw;
  length name_key $200;
  name_key = upcase(compbl(strip(securuties)));
run;

/* data3_app：有 companyName 與 companyID，一起正規化 */
%norm_company(ds=patent_raw, out=patent_n, idvar=companyID, namevar=companyName);

/* ========= 3) 以公司「名稱」左併（merge 為主）帶入 companyID ========= */
proc sql;
  create table merged_out as
  select 
      b.companyID length=32,
      a.securuties,
      a.foreign_company,
      a.dummy
  from merge_n as a
  left join patent_n as b
    on a.name_key = b.name_key
  ;
quit;

/* ========= 4) 去重 ========= */
/* 先依 securuties 去重（維持你原來的規則） */
proc sort data=merged_out nodupkey out=merged_unique_byname;
  by securuties;
run;
/* 再補一層：若同一 companyID 仍有多列，取第一列 */
proc sort data=merged_unique_byname out=merged_unique nodupkey;
  by companyID;
run;

/* ========= 5) 依 companyID 排序並匯出中繼表 ========= */
proc sort data=merged_unique out=merged_sorted;
  by companyID;
run;

proc export data=merged_sorted
  outfile="&foreign_xlsx"
  dbms=xlsx replace;
  sheet="data_fc_app"
  putnames=yes;
run;

/* ========= 6) 讀回中繼表與 data3_app，統一 companyID 型態 ========= */
proc import datafile="&foreign_xlsx"
    out=foreign_company_raw dbms=xlsx replace;
    sheet="data_fc_app";
    getnames=yes;
run;

proc import datafile="&prd3_xlsx"
    out=patent_regression_data3_app dbms=xlsx replace;
    sheet="data3_app";
    getnames=yes;
run;

/* foreign_company：把 dummy 改名成 foreign_dummy，並正規化 companyID */
data foreign_company;
  set foreign_company_raw;
  length companyID_c $32 __raw $64;
  __raw = strip(vvalue(companyID));
  __raw = translate(__raw
           ,'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
           ,'０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ');
  __raw = compress(__raw, ' '||'　'||byte(160));
  __raw = upcase(__raw);
  if prxmatch('/^[0-9]+$/', __raw) then companyID_c = put(input(__raw, ?? best32.), z6.);
  else companyID_c = __raw;
  drop companyID __raw;
  rename companyID_c = companyID dummy = foreign_dummy;
run;

/* patent3：正規化 companyID（與上面一致） */
%norm_company(ds=patent_regression_data3_app, out=patent3, idvar=companyID, namevar=companyName);

/* ========= 7) 合併（左併，以 patent3 為主） ========= */
proc sql;
  create table patent_regression_data5_app as
  select 
      a.*,
      b.foreign_company,
      b.foreign_dummy
  from patent3 as a
  left join foreign_company as b
    on a.companyID = b.companyID
  ;
quit;

/* ========= 8) 調整欄位順序 ========= */
data patent_regression_data5_app;
  retain 
      year companyID companyName
      invention_acc new_acc design_acc total_acc
      invention_sin new_sin design_sin total_sin
      ROE cost_to_income_ratio Zscore
      size FL current_ratio market_share ARTR PQ gGDP income_structure marginFac
      foreign_company foreign_dummy;
  set patent_regression_data5_app;
run;

/* ========= 9) 按 year、companyID 排序 ========= */
proc sort data=patent_regression_data5_app;
  by year companyID;
run;

/* === 10) 覆寫：直接用公司名稱比對（避免 ID 格式差異）=== */
data patent_regression_data5_app;
  set patent_regression_data5_app;
  length _name $200;
  _name = upcase(compbl(strip(companyName)));  /* 正規化公司名 */

  /* 30142 → 港商匯豐詹金寶證券亞 → 是, 0 */
  if _name = upcase("港商匯豐詹金寶證券亞") then do;
      foreign_company = '是';
      foreign_dummy = 0;
  end;

  /* 30869 → 新壽證券 → 否, 1 */
  if _name = upcase("新壽證券") then do;
      foreign_company = '否';
      foreign_dummy = 1;
  end;

  /* 000154 → 是, 0 */
  if companyID = '000154' then do;
      foreign_company = '是';
      foreign_dummy = 0;
  end;

  /* 000891 → 是, 0 */
  if companyID = '000891' then do;
      foreign_company = '是';
      foreign_dummy = 0;
  end;

  drop _name;
run;

/* ========= 11)（可選）刪除 foreign_* 缺失值 ========= */
data patent_regression_data5_app;
  set patent_regression_data5_app;
  if missing(foreign_company) or missing(foreign_dummy) then delete;
run;

/* ========= 12) 再排一次，保險 ========= */
proc sort data=patent_regression_data5_app;
  by year companyID;
run;

/* ========= 13) 匯出結果 ========= */
proc export data=patent_regression_data5_app
  outfile="&out5_xlsx"
  dbms=xlsx replace;
  sheet="data5_app";
  putnames=yes;
run;

/* 檢查筆數 */
%chk(patent_regression_data5_app);