/* 建議：先開啟錯誤、警告，跑完再關 */
OPTIONS NOTES STIMER SOURCE SYNTAXCHECK;

/* ========= 路徑 ========= */
%let main_xlsx = /home/u64061874/patent_regression_data1_apply.xlsx;   /* 來源檔（原值保留） */
%let out2_xlsx = /home/u64061874/patent_regression_data2_apply.xlsx;
%let out3_xlsx = /home/u64061874/patent_regression_data3_apply.xlsx;
%let out4_xlsx = /home/u64061874/patent_regression_data4_apply.xlsx;

/* 行數檢查小工具 */
%macro chk(ds);
  %local nobs;
  %if %sysfunc(exist(&ds)) %then %do;
    proc sql noprint; select count(*) into :nobs from &ds; quit;
    %put NOTE: &=ds has &nobs rows.;
  %end;
  %else %put NOTE: &=ds does not exist.;
%mend;

/* ========= 1) 讀主檔（自動挑工作表） ========= */
%macro load_main_xlsx(xlsx=&main_xlsx, out=patent_data);
  libname _m xlsx "&xlsx";
  %local _sheet;
  proc sql noprint;
    select memname into :_sheet trimmed
    from dictionary.tables
    where libname='_M' and upcase(memname) like '%APP%'
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

/* ========= 2) 只建數值輔助欄位（原值保留） ========= */
data patent_data_num;
  set patent_data;

  length _s $400;

  %macro mknum(src, dst);
    length &dst 8.;
    if vtype(&src)='N' then &dst=&src;
    else do;
      _s = strip(&src);
      _s = tranwrd(_s,'，',',');
      _s = compress(_s, ', '||byte(160));
      if _s in ('-', '—', 'N/A', 'NA', '.') then &dst = .;
      else if not missing(_s) and substr(_s,length(_s),1)='%' then do;
        _s = substr(_s,1,length(_s)-1);
        &dst = input(_s, ?? best32.)/100;
      end;
      else &dst = input(_s, ?? best32.);
    end;
  %mend;

  %mknum(ROA,                 nROA);
  %mknum(brokerage_income,    nBrokerage);
  %mknum(loan_income,         nLoan);
  %mknum(lending_income,      nLending);
  %mknum(underwriting_income, nUnderw);
  %mknum(stock_income,        nStock);
  %mknum(dividend_income,     nDividend);
  %mknum(futures_income,      nFutures);
  %mknum(securities_income,   nSecurities);
  %mknum(settlement_income,   nSettlement);
  %mknum(futures_management_income, nFutMgmt);
  %mknum(management_fee_income,     nMgmtFee);
  %mknum(consulting_fee_income,     nConsulting);

  %mknum(interest_income,     nIntIncome);
  %mknum(interest_cost,       nIntCost);

  %mknum(TA,                  nTA);
  %mknum(Equity,              nEquity);

  %mknum(GDP_growth,          nGDP);
  %mknum(labor_cost,          nLaborCost);
  %mknum(current_ratio,       nCurrentRatio);
  %mknum(ARTR,                nARTR);
  %mknum(avg_PQ_lag,          nPQ);

  drop _s;
run;

%chk(patent_data_num);

proc means data=patent_data_num noprint;
  var nROA;
  output out=roa_std(drop=_type_ _freq_) std=std_roa;
run;

data _null_;
  set roa_std;
  call symputx('std_roa', std_roa);
run;

/* ========= 3) 每年 brokerage_income 總和 ========= */
proc sort data=patent_data_num; by year; run;

proc summary data=patent_data_num nway;
  class year;
  var nBrokerage;
  output out=year_brokerage_total(drop=_type_ _freq_) sum=total_brokerage_income;
run;

/* ========= 4) 特徵計算 ========= */
data patent_final;
  merge patent_data_num(in=a)
        year_brokerage_total;
  by year;
  if a;

  total_income_components = sum(
      nBrokerage, nLoan, nLending,
      nUnderw, nStock, nDividend,
      nFutures, nSecurities, nSettlement,
      nFutMgmt, nMgmtFee, nConsulting
  );

  PQ   = nPQ;
  if nTA>0 then size = log(nTA); else size = .;
  gGDP = nGDP;
  if nEquity ne 0 then FL = nTA/nEquity; else FL = .;

  if &std_roa > 0 and nTA > 0 then Zscore = ((nEquity/nTA) + nROA) / &std_roa;
  else Zscore = .;

  if total_income_components>0 then income_structure = nBrokerage / total_income_components;
  else income_structure = .;

  if total_brokerage_income>0 then market_share = nBrokerage / total_brokerage_income;
  else market_share = .;

  cost_income_den = sum( (nIntIncome - nIntCost), total_income_components );
  if cost_income_den>0 then cost_to_income_ratio = nLaborCost / cost_income_den;
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

/* ========= 5) 排序 → data2 ========= */
proc sort data=patent_final
          out=patent_regression_data2_srt;
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

/* ========= 6) 匯出 data2 ========= */
proc export data=patent_regression_data2_srt
  outfile="&out2_xlsx"
  dbms=xlsx replace;
  sheet="data2_app";
  putnames=yes;
run;

/* ========= 7) 讀回 data2_app ========= */
proc import datafile="&out2_xlsx"
  out=patent_regression_data2
  dbms=xlsx replace;
  sheet="data2_app";
  getnames=yes;
run;

/* ========= 8) 匯入 marginFac ========= */
proc import out=marginFac
  datafile="/home/u64061874/marginFac.xlsx"
  dbms=xlsx replace;
  getnames=yes;
run;

/* ========= 9) 合併 → data3 ========= */
proc sql;
  create table patent_regression_data3 as
  select e.*,
         s.marginFac1, s.marginFac2
  from patent_regression_data2 as e
  left join marginFac as s
    on cats(e.companyID) = cats(s.companyID)
   and cats(e.year)      = cats(s.year);
quit;

proc sort data=patent_regression_data3;
  by year companyID;
run;

%chk(patent_regression_data3);

/* ========= 10) 篩選名單 → data4 ========= */
data patent_regression_data4;
  set patent_regression_data3;
  where cats(companyID) in (
    "000980","6008","6016","000960","000884","000616","6005",
    "000815","000116","000888","000930","0009A0","000885","000779",
    "2856","000025","2854","000102"
  );
run;

%chk(patent_regression_data4);

/* ========= 11) 匯出 data3 / data4 ========= */
proc export data=patent_regression_data3
  outfile="&out3_xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data3_apply";
  putnames=yes;
run;

proc export data=patent_regression_data4
  outfile="&out4_xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data4_apply";
  putnames=yes;
run;

/* 結束時可關閉訊息 */
OPTIONS NONOTES NOSTIMER NOSOURCE NOSYNTAXCHECK;