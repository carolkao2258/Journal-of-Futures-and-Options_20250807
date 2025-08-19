/* ========= 路徑 ========= */
%let main_xlsx = /home/u64061874/patent_regression_data1_apply.xlsx;
%let out2_xlsx = /home/u64061874/patent_regression_data2_apply.xlsx;
%let out3_xlsx = /home/u64061874/patent_regression_data3_apply.xlsx;
%let out4_xlsx = /home/u64061874/patent_regression_data4_apply.xlsx;

/* ========= 1) 匯入主資料 ========= */
proc import datafile="&main_xlsx"
    out=patent_data dbms=xlsx replace;
    getnames=yes;
run;

/* -- 1a) 確保 ROA、brokerage_income 可用於數值統計（若原為字元就轉數值） -- */
data patent_data;
  set patent_data;
  length __s $400;
  if vtype(ROA)='C' then do;
    __s = ktranslate(strip(vvalue(ROA)),' ','A0'x);
    __s = ktranslate(__s,',','，');
    __s = compress(__s,', ');
    ROA = input(__s, best32.);
  end;
  if vtype(brokerage_income)='C' then do;
    __s = ktranslate(strip(vvalue(brokerage_income)),' ','A0'x);
    __s = ktranslate(__s,',','，');
    __s = compress(__s,', ');
    brokerage_income = input(__s, best32.);
  end;
  drop __s;
run;

/* ========= 2) ROA 全樣本標準差（Zscore 用；單一數值） ========= */
proc means data=patent_data noprint;
    var ROA;
    output out=roa_std(drop=_type_ _freq_) std=std_roa;
run;

/* 存成巨集變數 */
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

/* ========= 4) 最終欄位計算（完全移除 marginFac 流程） ========= */
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

/* ========= 5) 排序並直接用 keep 白名單排除 marginFac → data2_app ========= */
proc sort data=patent_final 
          out=patent_regression_data2_app
          (keep=
              year companyID companyName
              invention_acc new_acc design_acc total_acc
              invention_sin new_sin design_sin total_sin
              ROE cost_to_income_ratio Zscore
              size FL current_ratio market_share ARTR PQ gGDP income_structure);
    by year companyID;
run;

data patent_regression_data2_app;
    retain
        year companyID companyName
        invention_acc new_acc design_acc total_acc
        invention_sin new_sin design_sin total_sin
        ROE cost_to_income_ratio Zscore
        size FL current_ratio market_share ARTR PQ gGDP income_structure;
    set patent_regression_data2_app;
run;

/* ========= 6) 匯出 data2_app ========= */
proc export data=patent_regression_data2_app
    outfile="&out2_xlsx"
    dbms=xlsx replace;
    sheet="data2_app";
    putnames=yes;
run;

/* ========= 7) 讀回 data2_app 作為後續來源（指定相同 sheet） ========= */
proc import datafile="&out2_xlsx"
    out=patent_regression_data2_app
    dbms=xlsx replace;
    sheet="data2_app";
    getnames=yes;
run;

/* ========= 7b) 正規化 companyID/year（避免與 marginFac 型別不合） ========= */
%macro norm_keys(in=, out=, idvar=companyID, yearvar=year);
data &out;
  set &in;
  length __raw $64 &idvar._key $32;
  __raw = strip(vvalue(&idvar));
  __raw = translate(__raw
           ,'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
           ,'０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ');
  __raw = compress(__raw, ' '||'　'||byte(160));
  __raw = upcase(__raw);
  if prxmatch('/^[0-9]+$/', __raw) then &idvar._key = put(input(__raw, ?? best32.), z6.);
  else &idvar._key = __raw;
  length &yearvar._key 8;
  &yearvar._key = input(strip(vvalue(&yearvar)), ?? best32.);
  drop &idvar &yearvar __raw;
  rename &idvar._key=&idvar
         &yearvar._key=&yearvar;
run;
%mend;

%norm_keys(in=patent_regression_data2_app, out=e_norm, idvar=companyID, yearvar=year);

/* ========= 8) 匯入 marginFac、正規化，合併 → data3_app ========= */
proc import out=marginFac_raw
     datafile="/home/u64061874/marginFac.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;
%norm_keys(in=marginFac_raw, out=s_norm, idvar=companyID, yearvar=year);

proc sql;
    create table patent_regression_data3_app as
    select e.*, s.marginFac
    from e_norm as e
    left join s_norm as s
      on e.companyID = s.companyID 
     and e.year      = s.year;
quit;

/* 若需要：刪除 marginFac 缺失值 */
data patent_regression_data3_app;
    set patent_regression_data3_app;
    if missing(marginFac) then delete;
run;

proc sort data=patent_regression_data3_app;
    by year companyID;
run;

/* ========= 9) 建立名單並正規化 → 篩出 data4_app ========= */
data keep_ids_raw;
  length companyID $32;
  infile datalines truncover;
  input companyID $32.;
  datalines;
000980
6008
6016
000960
000884
000616
6005
000815
000116
000888
000930
0009A0
000885
000779
2856
000102
000025
2854
;
run;
%norm_keys(in=keep_ids_raw, out=keep_ids, idvar=companyID, yearvar=year);

proc sql;
  create table patent_regression_data4_app as
  select e.*
  from patent_regression_data3_app as e
  inner join keep_ids as k
    on e.companyID = k.companyID;
quit;

/* ========= 10) 匯出結果（_apply / data*_app） ========= */
proc export data=patent_regression_data3_app
    outfile="&out3_xlsx"
    dbms=xlsx replace;
    sheet="data3_app";
    putnames=yes;
run;

proc export data=patent_regression_data4_app
    outfile="&out4_xlsx"
    dbms=xlsx replace;
    sheet="data4_app";
    putnames=yes;
run;