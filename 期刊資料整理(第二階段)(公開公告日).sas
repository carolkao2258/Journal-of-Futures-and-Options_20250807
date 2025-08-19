/* ========= 路徑 ========= */
%let main_xlsx = /home/u64061874/patent_regression_data1_announce.xlsx;   /* 來源檔 */
%let out2_xlsx = /home/u64061874/patent_regression_data2_announce.xlsx;
%let out3_xlsx = /home/u64061874/patent_regression_data3_announce.xlsx;
%let out4_xlsx = /home/u64061874/patent_regression_data4_announce.xlsx;

/* 行數檢查小工具（會在 LOG 顯示筆數） */
%macro chk(ds);
  %local nobs; 
  %if %sysfunc(exist(&ds)) %then %do;
    proc sql noprint; select count(*) into :nobs from &ds; quit;
    %put NOTE: &=ds has &nobs rows.;
  %end;
  %else %put NOTE: &=ds does not exist.;
%mend;

/* ========= 1) 讀主檔：用 LIBNAME 自動挑選正確工作表 ========= */
/* 規則：優先找名字含 ANN 的工作表；沒有就取第一個 */
%macro load_main_xlsx(xlsx=&main_xlsx, out=patent_data);
  libname _m xlsx "&xlsx";
  %local _sheet;
  proc sql noprint;
    /* 先找包含 ANN 的表 */
    select memname into :_sheet trimmed
    from dictionary.tables
    where libname='_M' and upcase(memname) like '%ANN%' 
    order by memname;
  quit;
  %if "&_sheet" = "" %then %do;
    proc sql noprint outobs=1;
      select memname into :_sheet trimmed
      from dictionary.tables
      where libname='_M'
      order by memname;
    quit;
  %end;
  %put NOTE: Picked sheet &_sheet from &xlsx.;
  data &out;
    set _m.&_sheet;
  run;
  libname _m clear;
%mend;

%load_main_xlsx(out=patent_data);
%chk(patent_data);

/* ========= 2) ROA 標準差（Zscore 用） ========= */
proc means data=patent_data noprint;
  var ROA;
  output out=roa_std(drop=_type_ _freq_) std=std_roa;
run;

data _null_;
  set roa_std;
  call symputx('std_roa', std_roa);
run;

/* ========= 3) 每年 brokerage_income 總和（market_share 分母） ========= */
proc sort data=patent_data; by year; run;

proc summary data=patent_data nway;
  class year;
  var brokerage_income;
  output out=year_brokerage_total(drop=_type_ _freq_) sum=total_brokerage_income;
run;

/* ========= 4) 特徵計算 ========= */
data patent_final;
  merge patent_data(in=a)
        year_brokerage_total;
  by year;
  if a;

  total_income_components = sum(
      brokerage_income, loan_income, lending_income,
      underwriting_income, stock_income, dividend_income,
      futures_income, securities_income, settlement_income,
      futures_management_income, management_fee_income, consulting_fee_income
  );

  PQ   = avg_PQ_lag;
  if TA>0 then size = log(TA); else size = .;
  gGDP = GDP_growth;
  if Equity ne 0 then FL = TA/Equity; else FL = .;

  if &std_roa > 0 and TA > 0 then Zscore = ((Equity/TA) + ROA) / &std_roa;
  else Zscore = .;

  if total_income_components>0 then income_structure = brokerage_income / total_income_components;
  else income_structure = .;

  if total_brokerage_income>0 then market_share = brokerage_income / total_brokerage_income;
  else market_share = .;

  cost_income_den = sum( (interest_income - interest_cost), total_income_components );
  if cost_income_den>0 then cost_to_income_ratio = labor_cost / cost_income_den;
  else cost_to_income_ratio = .;

  total_sin = sum(invention_sin, new_sin, design_sin);

  keep 
      year companyID companyName
      invention_acc new_acc design_acc total_acc
      invention_sin new_sin design_sin total_sin
      ROE cost_to_income_ratio Zscore
      size FL current_ratio market_share ARTR PQ gGDP income_structure;
run;
%chk(patent_final);

/* ========= 5) 排序 → data2  ========= */
proc sort data=patent_final 
          out=patent_regression_data2_srt
          (keep=
            year companyID companyName
            invention_acc new_acc design_acc total_acc
            invention_sin new_sin design_sin total_sin
            ROE cost_to_income_ratio Zscore
            size FL current_ratio market_share ARTR PQ gGDP income_structure);
  by year companyID;
run;

data patent_regression_data2_srt;
  retain
    year companyID companyName
    invention_acc new_acc design_acc total_acc
    invention_sin new_sin design_sin total_sin
    ROE cost_to_income_ratio Zscore
    size FL current_ratio market_share ARTR PQ gGDP income_structure;
  set patent_regression_data2_srt;
run;
%chk(patent_regression_data2_srt);

/* ========= 6) 匯出 data2_announce（sheet 用短名，避免 31 字限制） ========= */
proc export data=patent_regression_data2_srt
  outfile="&out2_xlsx"
  dbms=xlsx replace;
  sheet="data2_ann";
  putnames=yes;
run;

/* ========= 7) 讀回 data2_announce（指定相同 sheet）做後續合併 ========= */
proc import datafile="&out2_xlsx"
  out=patent_regression_data2
  dbms=xlsx replace;
  sheet="data2_ann";
  getnames=yes;
run;
%chk(patent_regression_data2);

/* ========= 8) 匯入 marginFac，合併 → data3  ========= */
proc import out=marginFac
  datafile="/home/u64061874/marginFac.xlsx"
  dbms=xlsx replace;
  getnames=yes;
run;

proc sql;
  create table patent_regression_data3 as
  select e.*, s.marginFac
  from patent_regression_data2 as e
  left join marginFac as s
    on e.companyID = s.companyID 
   and e.year      = s.year;
quit;

data patent_regression_data3;
  set patent_regression_data3;
  if missing(marginFac) then delete;
run;

proc sort data=patent_regression_data3;
  by year companyID;
run;
%chk(patent_regression_data3);

/* ========= 9) 依名單篩選 → data4 ========= */
data patent_regression_data4;
  set patent_regression_data3;
  where companyID in (
    "000980","6008","6016","000960","000884","000616","6005",
    "000815","000116","000888","000930","0009A0","000885","000779",
    "2856","000025","2854"
  );
run;
%chk(patent_regression_data4);

/* ========= 10) 匯出 data3/data4（sheet 用短名） ========= */
proc export data=patent_regression_data3
  outfile="&out3_xlsx"
  dbms=xlsx replace;
  sheet="data3_ann";
  putnames=yes;
run;

proc export data=patent_regression_data4
  outfile="&out4_xlsx"
  dbms=xlsx replace;
  sheet="data4_ann";
  putnames=yes;
run;