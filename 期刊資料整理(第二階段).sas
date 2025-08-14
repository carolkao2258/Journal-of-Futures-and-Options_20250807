/* ========= 路徑 ========= */
%let main_xlsx = /home/u64061874/patent_regression_data1.xlsx;
%let out_xlsx  = /home/u64061874/patent_regression_data2.xlsx;

/* ========= 1) 匯入主資料 ========= */
proc import datafile="&main_xlsx"
    out=patent_data dbms=xlsx replace;
    getnames=yes;
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

/* ========= 5) 排序並直接用 keep 白名單排除 marginFac ========= */
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

/* ========= 6) 匯出 ========= */
proc export data=patent_regression_data2_srt
    outfile="&out_xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data2";
    putnames=yes;
run;

/* ========= 1) 匯入 patent_regression_data2 ========= */
proc import datafile="/home/u64061874/patent_regression_data2.xlsx"
    out=patent_regression_data2
    dbms=xlsx replace;
    getnames=yes;
run;

/* ========= 2) 匯入 marginFac ========= */
proc import out=marginFac
     datafile="/home/u64061874/marginFac.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ========= 3) 用 PROC SQL 合併 ========= */
proc sql;
    create table patent_regression_data3 as
    select e.*, s.marginFac
    from patent_regression_data2 as e
    left join marginFac as s
    on e.companyID = s.companyID 
       and e.year = s.year;
quit;

/* ========= 4) 刪除 marginFac 缺失值 ========= */
data patent_regression_data3;
    set patent_regression_data3;
    if missing(marginFac) then delete;
run;

/* ========= 5) 依 year、companyID 排序 ========= */
proc sort data=patent_regression_data3;
    by year companyID;
run;

/* 保留指定 companyID */
data patent_regression_data4;
    set patent_regression_data3;
    where companyID in (
        "000980","6008","6016","000960","000884","000616","6005",
        "000815","000116","000888","000930","0009A0","000885","000779",
        "2856","000102","000025","2854"
    );
run;

/* 輸出結果 */
proc export data=patent_regression_data4
    outfile="/home/u64061874/patent_regression_data4.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data4";
    putnames=yes;
run;

/* ========= 6) 匯出結果 ========= */
proc export data=patent_regression_data3
    outfile="/home/u64061874/patent_regression_data3.xlsx"
    dbms=xlsx replace;
    sheet="patent_regression_data3";
    putnames=yes;
run;