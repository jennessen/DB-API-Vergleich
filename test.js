function validate(dbSet, apiSet) {
    // beliebiges Logging:

    let db_cJfsku = dbSet.db_cJfsku;
    let api_jfsku = apiSet.api_jfsku;
    console.log("check", db_cJfsku, "vs", api_jfsku);
    if (api_jfsku === null) {
       console.log("No Jfsku")
       return { ok: false, msg: "NoJFSKU" };
    }
    if(dbSet.db_Condition !== apiSet.api_condition){
        let api_fix;
        if(dbSet.db_Condition === "Default" ){
            if(apiSet.api_condition === "Unknown" ){
                api_fix = "Invoke-RestMethod 'https://ffnqa-api.office.jtl-software.de/api/v1/merchant/products/" + api_jfsku + "' -Method 'PATCH' -Headers $headers -Body '{ \"condition\" : \"Default\" }'" 
            }
        }
        return { ok: false, msg: dbSet.db_Condition + "!==" + apiSet.api_condition , api_fix:api_fix};
    }
    let wawi_fix = "UPDATE tArtikel SET cJfsku = '" + db_cJfsku + "' WHERE kArtikel=" + dbSet.db_kArtikel;

    var msg = "JFSKU: " + db_cJfsku + " ok"
    return { ok: true, msg: msg, wawi_fix: wawi_fix };
}
