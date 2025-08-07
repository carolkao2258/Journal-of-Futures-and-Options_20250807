/* 1. 匯入資料 */
proc import datafile="/home/u64061874/patent_regression_data1.xlsx"
    out=patent_data
    dbms=xlsx
    replace;
    getnames=yes;
run;

/* 2. 計算 ROA 的全樣本標準差 */
proc means data=patent_data noprint;
    var ROA;
    output out=roa_std (drop=_type_ _freq_) std=std_roa;
run;

/* 3. 合併 ROA 標準差到原始資料集 */
data patent_data2;
    if _N_ = 1 then set roa_std;
    set patent_data;
run;

/* 4. 計算新變數，保留與排序指定欄位 */
data patent_final;
    set patent_data2;
    PQ   = avg_PQ_lag;                /* PQ */
    size = log(TA);                   /* ln(TA) */
    gGDP = GDP_growth;                /* gGDP */
    FL   = TA / Equity;               /* 財務槓桿 */
    Zscore = ((Equity / TA) + ROA) / std_roa; /* Z-score 公式 */
    keep year companyID companyName invention new design total ROE Zscore PQ size gGDP ARTR FL;
run;

/* 5. 依 year 與 companyID 排序 */
proc sort data=patent_final out=patent_regression_data2;
    by year companyID;
run;

/* 6. 匯出結果到 Excel */
proc export data=patent_regression_data2
    outfile="/home/u64061874/patent_regression_data2.xlsx"
    dbms=xlsx
    replace;
    sheet="result";
run;
