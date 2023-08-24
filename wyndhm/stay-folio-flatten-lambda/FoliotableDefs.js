const jpath = require('jspath');


function extract(path, o) {
    let r = jpath.apply(path, o);
    if (typeof r === "undefined") {
        return '';
    } else {
        return r;
    }
}

let StaytableDefs = [{

    tableName: "stg_folio_rsrv_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Version = extract('..Hotel.Version[0]',json);
        let Reservations = extract('..Reservations',json);

        let records = Reservations.map((Reservations,Reservations_Index) => {
            return extract('.Reservation',Reservations).map((Reservation,Reservation_Index) => {
                let is_tax_exmpt = extract('.IsTaxExempt[0]', Reservation);
                let is_tax_exmpt_dscr = '';
                if (is_tax_exmpt == 0){
                    is_tax_exmpt_dscr = "100% tax exempt";
                }
                else if (is_tax_exmpt == 1){
                    is_tax_exmpt_dscr = "fully taxable";
                }
                else if (is_tax_exmpt == 2){
                    is_tax_exmpt_dscr =  "partially taxable";
                }
                return {
                    Reservation_seq_no: String(Reservation_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    Version: Version,
                    pms_conf_num: extract('.PMSID[0]', Reservation),
                    conf_num: extract('.CRSID[0]', Reservation),
                    trans_id: extract('.TransactionId[0]', Reservation),
                    chrg_dt: extract('.ChargeDate[0]', Reservation),
                    chrg_tm: extract('.ChargeTime[0]', Reservation),
                    chrg_ts: extract('.ChargeDate[0]', Reservation)+' '+ extract('.ChargeTime[0]', Reservation),
                    chrg_cd: extract('.ChargeCode[0]', Reservation),
                    chrg_dscr: extract('.Description[0]', Reservation),
                    trans_typ: extract('.TransactionType[0]', Reservation),
                    bse_typ: extract('.BaseType[0]', Reservation),
                    edw_typ: extract('.EDWType[0]', Reservation),
                    cc_lstfour_dgt: extract('.CCID[0]', Reservation),
                    gl_cd: extract('.GLCode[0]', Reservation),
                    amt: extract('.Amount[0]', Reservation),
                    frm_folio_acct_num: extract('.FromAccount[0]', Reservation),
                    frm_crs_conf_num: extract('.FromCRSConf[0]', Reservation),
                    is_audt_pstng: extract('.IsAuditPosting[0]', Reservation),
                    tax_on_itm_id: extract('.TaxOnItemId[0]', Reservation),
                    adjtd_itm_id: extract('.AdjustedItemId[0]', Reservation),
                    is_tax_exmpt: is_tax_exmpt,
                    is_tax_exmpt_dscr: is_tax_exmpt_dscr,
                    pms_cmpny_acct_id: extract('.CompPMSID[0]', Reservation),
                    pms_cmpny_acct_nm: extract('.CompPMSName[0]', Reservation),
                    pms_group_id: extract('.GroupPMSID[0]', Reservation),
                    pms_group_nm: extract('.GroupPMSName[0]', Reservation),
                    clrk_id: extract('.ClerkID[0]', Reservation),
                    clrk_nm: extract('.ClerkName[0]', Reservation),
                    insert_ts: '',
                    file_nm: s3outputPath
                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_cmpny_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Companies = extract('..Companies',json);

        let records = Companies.map((Companies,Companies_Index) => {
            return extract('.Company',Companies).map((Company,Company_Index) => {
                let is_tax_exmpt = extract('.IsTaxExempt[0]', Company);
                let is_tax_exmpt_dscr = '';
                if (is_tax_exmpt == 0){
                    is_tax_exmpt_dscr = "100% tax exempt";
                }
                else if (is_tax_exmpt == 1){
                    is_tax_exmpt_dscr = "fully taxable";
                }
                else if (is_tax_exmpt == 2){
                    is_tax_exmpt_dscr =  "partially taxable";
                }
                return {
                    Company_seq_no: String(Company_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    pms_conf_num: extract('.PMSID[0]', Company),
                    pms_cmpny_acct_nm: extract('.Name[0]', Company),
                    crs_cmpny_acct_id: extract('.CompanyId[0]', Company),
                    trans_id: extract('.TransactionId[0]', Company),
                    chrg_dt: extract('.ChargeDate[0]', Company),
                    chrg_tm: extract('.ChargeTime[0]', Company),
                    chrg_ts: extract('.ChargeDate[0]', Company)+' '+ extract('.ChargeTime[0]', Company),
                    chrg_cd: extract('.ChargeCode[0]', Company),
                    chrg_dscr: extract('.Description[0]', Company),
                    trans_typ: extract('.TransactionType[0]', Company),
                    base_typ: extract('.BaseType[0]', Company),
                    edw_typ: extract('.EDWType[0]', Company),
                    cc_lstfour_dgt: extract('.CCID[0]', Company),
                    gl_cd: extract('.GLCode[0]', Company),
                    amt: extract('.Amount[0]', Company),
                    frm_folio_acct_num: extract('.FromAccount[0]', Company),
                    frm_crs_conf_num: extract('.FromCRSConf[0]', Company),
                    is_audt_pstng: extract('.IsAuditPosting[0]', Company),
                    tax_on_itm_id: extract('.TaxOnItemId[0]', Company),
                    adjstd_itm_id: extract('.AdjustedItemId[0]', Company),
                    is_tax_exmpt: is_tax_exmpt,
                    is_tax_exmpt_dscr: is_tax_exmpt_dscr,
                    clrk_id: extract('.ClerkID[0]', Company),
                    clrk_nm: extract('.ClerkName[0]', Company),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_grp_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Groups = extract('..Groups',json);

        let records = Groups.map((Groups,Groups_Index) => {
            return extract('.Group',Groups).map((Group,Group_Index) => {
                let is_tax_exmpt = extract('.IsTaxExempt[0]', Group);
                let is_tax_exmpt_dscr = '';
                if (is_tax_exmpt == 0){
                    is_tax_exmpt_dscr = "100% tax exempt";
                }
                else if (is_tax_exmpt == 1){
                    is_tax_exmpt_dscr = "fully taxable";
                }
                else if (is_tax_exmpt == 2){
                    is_tax_exmpt_dscr =  "partially taxable";
                }
                return {
                    Group_seq_no: String(Group_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    pms_conf_num: extract('.PMSID[0]', Group),
                    pms_cmpny_acct_nm: extract('.Name[0]', Group),
                    crs_cmpny_acct_id: extract('.CompanyId[0]', Group),
                    pms_group_id: extract('.GroupId[0]', Group),
                    trans_id: extract('.TransactionId[0]', Group),
                    chrg_dt: extract('.ChargeDate[0]', Group),
                    chrg_tm: extract('.ChargeTime[0]', Group),
                    chrg_ts: extract('.ChargeDate[0]', Group)+' '+ extract('.ChargeTime[0]', Group),
                    chrg_cd: extract('.ChargeCode[0]', Group),
                    chrg_dscr: extract('.Description[0]', Group),
                    trans_typ: extract('.TransactionType[0]', Group),
                    base_typ: extract('.BaseType[0]', Group),
                    edw_typ: extract('.EDWType[0]', Group),
                    cc_lstfour_dgt: extract('.CCID[0]', Group),
                    gl_cd: extract('.GLCode[0]', Group),
                    amt: extract('.Amount[0]', Group),
                    frm_acct: extract('.FromAccount[0]', Group),
                    frm_crs_conf_num: extract('.FromCRSConf[0]', Group),
                    is_audt_pstng: extract('.IsAuditPosting[0]', Group),
                    tax_on_itm_id: extract('.TaxOnItemId[0]', Group),
                    adjstd_itm_id: extract('.AdjustedItemId[0]', Group),
                    is_tax_exmpt: is_tax_exmpt,
                    is_tax_exmpt_dscr: is_tax_exmpt_dscr,
                    clrk_id: extract('.ClerkID[0]', Group),
                    clrk_nm: extract('.ClerkName[0]', Group),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_comp_ar_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let CompanyARs = extract('..CompanyARs',json);

        let records = CompanyARs.map((CompanyARs,CompanyARs_Index) => {
            return extract('.CompanyAR',CompanyARs).map((CompanyAR,CompanyAR_Index) => {
                let is_tax_exmpt = extract('.IsTaxExempt[0]', CompanyAR);
                let is_tax_exmpt_dscr = '';
                if (is_tax_exmpt == 0){
                    is_tax_exmpt_dscr = "100% tax exempt";
                }
                else if (is_tax_exmpt == 1){
                    is_tax_exmpt_dscr = "fully taxable";
                }
                else if (is_tax_exmpt == 2){
                    is_tax_exmpt_dscr =  "partially taxable";
                }
                return {
                    CompanyAR_seq_no: String(CompanyAR_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    pms_conf_num: extract('.PMSID[0]', CompanyAR),
                    pms_corp_acct_nm: extract('.Name[0]', CompanyAR),
                    pms_corp_acct_id: extract('.CompanyId[0]', CompanyAR),
                    trans_id: extract('.TransactionId[0]', CompanyAR),
                    chrg_dt: extract('.ChargeDate[0]', CompanyAR),
                    chrg_tm: extract('.ChargeTime[0]', CompanyAR),
                    chrg_ts: extract('.ChargeDate[0]', CompanyAR)+' '+ extract('.ChargeTime[0]', CompanyAR),
                    chrg_cd: extract('.ChargeCode[0]', CompanyAR),
                    chrg_dscr: extract('.Description[0]', CompanyAR),
                    trans_typ: extract('.TransactionType[0]', CompanyAR),
                    base_typ: extract('.BaseType[0]', CompanyAR),
                    edw_typ: extract('.EDWType[0]', CompanyAR),
                    cc_id: extract('.CCID[0]', CompanyAR),
                    gl_cd: extract('.GLCode[0]', CompanyAR),
                    amt: extract('.Amount[0]', CompanyAR),
                    from_folio_acct_num: extract('.FromAccount[0]', CompanyAR),
                    from_crs_conf_num: extract('.FromCRSConf[0]', CompanyAR),
                    tax_on_itm_id: extract('.TaxOnItemId[0]', CompanyAR),
                    adjstd_itm_id: extract('.AdjustedItemId[0]', CompanyAR),
                    uniq_invce_id: extract('.UniqueInvoiceID[0]', CompanyAR),
                    is_tax_exmpt: is_tax_exmpt,
                    is_tax_exmpt_dscr: is_tax_exmpt_dscr,
                    clrk_id: extract('.ClerkID[0]', CompanyAR),
                    clrk_nm: extract('.ClerkName[0]', CompanyAR),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_ldgr_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let HotelLedgers = extract('..HotelLedgers',json);

        let records = HotelLedgers.map((HotelLedgers,HotelLedgers_Index) => {
            return extract('.HotelLedger',HotelLedgers).map((HotelLedger,HotelLedger_Index) => {
                
                return {
                    HotelLedger_seq_no: String(HotelLedger_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    ldgr_dt: extract('.Date[0]', HotelLedger),
                    advnc_dpst_ldgr: extract('.ADL[0]', HotelLedger),
                    guest_ldgr: extract('.GL[0]', HotelLedger),
                    cmpny_ldgr: extract('.CL[0]', HotelLedger),
                    group_ldgr: extract('.GRPL[0]', HotelLedger),
                    ar_cty_ldgr: extract('.ARL[0]', HotelLedger),
                    insert_ts:'',
                    file_nm:s3outputPath

                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_statstcs_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Statistics = extract('..Statistics',json);

        let records = Statistics.map((Statistics,Statistics_Index) => {
            
                return {
                    CompanyAR_seq_no: String(Statistics_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    tot_rms: extract('.TotalRooms[0]', Statistics),
                    ooo_rms: extract('.OOORooms[0]', Statistics),
                    ooi_rms: extract('.OOIRooms[0]', Statistics),
                    rvnue_rms: extract('.RevenueRooms[0]', Statistics),
                    comp_rms: extract('.CompRooms[0]', Statistics),
                    zero_rt_rms: extract('.ZeroRateRooms[0]', Statistics),
                    day_use_rms: extract('.DayUseRooms[0]', Statistics),
                    rsrvd_rms: extract('.Reserved[0]', Statistics),
                    num_of_walkins: extract('.Walkins[0]', Statistics),
                    no_show_rms: extract('.NoShows[0]', Statistics),
                    num_of_Cancel: extract('.Cancellations[0]', Statistics),
                    erly_chkins: extract('.EarlyCheckin[0]', Statistics),
                    erly_chkouts: extract('.EarlyCheckout[0]', Statistics),
                    txble_rm_rev: extract('.TaxableRoomRev[0]', Statistics),
                    non_txble_rm_rev: extract('.NonTaxableRoomRev[0]', Statistics),
                    prt_txble_rm_rev: extract('.PartTaxableRoomRev[0]', Statistics),
                    tax_rev: extract('.TaxRev[0]', Statistics),
                    fb_rev: extract('.FBRev[0]', Statistics),
                    other_rev: extract('.OtherRev[0]', Statistics),
                    csh_rev: extract('.CashRev[0]', Statistics),
                    cc_rev: extract('.CCRev[0]', Statistics),
                    db_rev: extract('.DBRev[0]', Statistics),
                    tot_rms_sld: extract('.TotalRoomSold[0]', Statistics),
                    comp_rms_sld: extract('.CompRoomSold[0]', Statistics),
                    rm_rev_no_tx: extract('.RoomRevNoTax[0]', Statistics),
                    rm_rev_and_tx: extract('.RoomRevAndTax[0]', Statistics),
                    rm_rev_adj: extract('.RoomRevAdj[0]', Statistics),
                    no_show_crs: extract('.NoShowCRS[0]', Statistics),
                    no_show_prp: extract('.NoShowProp[0]', Statistics),
                    cancel_crs: extract('.CancelCRS[0]', Statistics),
                    cancel_prp: extract('.CancelProp[0]', Statistics),
                    fns_occupd: extract('.FNSOccupied[0]', Statistics),
                    prcnt_occupd_no_fns: extract('.PercentOccupiedNoFNS[0]', Statistics),
                    prcnt_occupd_fns: extract('.PercentOccupiedFNS[0]', Statistics),
                    totl_rm_sld_no_fns: extract('.TotalRoomSoldNOFNS[0]', Statistics),
                    adr_wth_fns: extract('.ADRWithFNS[0]', Statistics),
                    adr_no_fns: extract('.ADRNoFNS[0]', Statistics),
                    rm_rev_adj_allw: extract('.RoomRevAdjAllow[0]', Statistics),
                    rm_rev_adj_disallw: extract('.RoomRevAdjDisAllow[0]', Statistics),
                    fxd_flg: extract('.FixedFlag[0]', Statistics),
                    day_use_rms_paid: extract('.DayUseRoomsPaid[0]', Statistics),
                    day_use_rms_unpaid: extract('.DayUseRoomsUnPaid[0]', Statistics),
                    day_use_rms_paid_rev: extract('.DayUseRoomsPaidRev[0]', Statistics),
                    noshow_rms_paid: extract('.NoShowRoomsPaid[0]', Statistics),
                    no_show_rms_unpaid: extract('.NoShowRoomsUnPaid[0]', Statistics),
                    no_show_rms_rev: extract('.NoShowRoomsRev[0]', Statistics),
                    othrs_paid_rms: extract('.OthersPaidRoom[0]', Statistics),
                    othrs_paid_rm_rev: extract('.OtherPaidRoomRev[0]', Statistics),
                    inhouse_paid_rms: extract('.InHousePaidRoom[0]', Statistics),
                    inhouse_paid_rev: extract('.InHousePaidRev[0]', Statistics),
                    totl_paid_rm: extract('.TotalPaidRoom[0]', Statistics),
                    rm_rev_no_fns: extract('.RoomRevNoFNS[0]', Statistics),
                    rembrsmnt_fns_with_tax: extract('.ReimbursementFNSWithTax[0]', Statistics),
                    rembrsmnt_fns_no_tax: extract('.ReimbursementFNSNoTax[0]', Statistics),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
        
        return records; 
    }
},

{
    tableName: "stg_folio_tax_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Taxes = extract('..Taxes.Tax',json);

        let records = Taxes.map((Taxes,Taxes_Index) => {
                return {
                    Taxes_seq_no: String(Taxes_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    tax_cd: extract('.TaxCode[0]', Taxes),
                    tax_dscr: extract('.Description[0]', Taxes),
                    is_prcnt: extract('.IsPercentage[0]', Taxes),
                    amt: extract('.Amount[0]', Taxes),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
       
        return records; 
    }
},
{
    tableName: "stg_folio_tax_cd",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Charges = extract('..Charges.Charge',json);

        let records = Charges.map((Charges,Charges_Index) => {
            return extract('.TaxCodes.TaxCode',Charges).map((TaxCodes,TaxCodes_Index) => {
                return {
                    Charges_seq_no: String(Charges_Index + 1),
                    TaxCodes_seq_no: String(TaxCodes_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    chrg_cd: extract('.ChargeCode[0]', Charges),
                    tax_cds: '',
                    tax_cd: extract('.[0]', TaxCodes),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
        });
        return records; 
    }
},
{
    tableName: "stg_folio_rsrv_gust_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let Guests = extract('..Guests.Guest',json);

        let records = Guests.map((Guests,Guests_Index) => {
                return {
                    Guests_seq_no: String(Guests_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    pms_conf_num: extract('.PMSID[0]', Guests),
                    conf_num: extract('.CRSID[0]', Guests),
                    trvl_agnt_id: extract('.TravelAgentID1[0]', Guests),
                    trvl_agnt_id_2: extract('.TravelAgentID2[0]', Guests),
                    num_of_adlts: extract('.Adult[0]', Guests),
                    num_of_chldrn: extract('.Child[0]', Guests),
                    Num_of_rms: extract('.NoOfRoom[0]', Guests),
                    rsrv_orig_bkng_ts: extract('.CreateDate[0]', Guests),
                    rsrv_lst_mdfy_ts: extract('.LastUpdate[0]', Guests),
                    rm_num: extract('.Room[0]', Guests),
                    rm_typ: extract('.RoomType[0]', Guests),
                    rsrv_notes: extract('.Notes[0]', Guests),
                    arvl_dt: extract('.Arrival[0]', Guests),
                    deptr_dt: extract('.Departure[0]', Guests),
                    rsrv_sts: extract('.Status[0]', Guests),
                    rt_pln_cd: extract('.RatePlan[0]', Guests),
                    rt_pln_dscr: extract('.RatePlanDesc[0]', Guests),
                    mrkt_seg: extract('.MarketSegment[0]', Guests),
                    member_num: extract('.MemberNo[0]', Guests),
                    guest_nm: extract('.Name[0]', Guests),
                    guest_email: extract('.Email[0]', Guests),
                    guest_addr: extract('.Address[0]', Guests),
                    guest_phn: extract('.Phone[0]', Guests),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
       
        return records; 
    }
},
{
    tableName: "stg_folio_mrkt_seg_dtl",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
        let RevByMktSeg = extract('..RevByMktSeg.Revenue',json);

        let records = RevByMktSeg.map((RevByMktSeg,RevByMktSeg_Index) => {
                return {
                    RevByMktSeg_seq_no: String(RevByMktSeg_Index + 1),
                    brnd_id: brnd_id,
                    site_id: site_id,
                    Bus_dt: Bus_dt,
                    mrkt_seg: extract('.MktSeg[0]', RevByMktSeg),
                    tax_rev_amt: extract('.TAX[0]', RevByMktSeg),
                    othr_rm_amt: extract('.OTHER[0]', RevByMktSeg),
                    rm_rev_amt: extract('.ROOM[0]', RevByMktSeg),
                    num_of_ngts: extract('.Nights[0]', RevByMktSeg),
                    insert_ts:'',
                    file_nm:s3outputPath
                    
                };
                
            })
            
       
        return records; 
    }
},
{
    tableName: "stg_folio_btch_hdr",
    transform: function(json,s3outputPath,seqNum) {
        let brnd_id = extract('..Hotel.Brand[0]',json);
        let site_id = extract('..Hotel.Id[0]',json);
        let Bus_dt = extract('..Hotel.BusinessDate[0]',json);
         //changed logic to avoid multiple batch header records
        let hotel = extract('..Hotel',json);

        let records = hotel.map((hotel,hotel_Index) => {
                return {
                    btch_num: '',
                    site_id: site_id,
                    brand_id: brnd_id,
                    Bus_dt: Bus_dt,
                    version: '',
                    stg_load_strt_ts: '',
                    edw_load_ts: '',
                    stg_sts: '',
                    edw_sts: '',
                    src_file_nm:s3outputPath,
                    create_by: 'GLUE',
                    create_ts: '',
                    updt_usr: '',
                    updt_ts: '',                   
                    
                };
                
            })
            
       
        return records; 
    }
}
];

module.exports = StaytableDefs;