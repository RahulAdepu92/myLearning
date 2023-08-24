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

    tableName: "stg_stay_guest",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let records = CENDANTUMFCN.map((CENDANTUMFCN,CENDANTUMFCN_Index) => {
            return {
                stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN),
                frst_nm: extract('.cname.FNM[0]', CENDANTUMFCN),
                mid_nm: extract('.cname.MNM[0]', CENDANTUMFCN),
                lst_nm: extract('.cname.LNM[0]', CENDANTUMFCN),
                nm_prfx: extract('.cname.NPX[0]', CENDANTUMFCN),
                nm_sufx: extract('.cname.NSX[0]', CENDANTUMFCN),
                gnrc_nm: extract('.cname.GNM[0]', CENDANTUMFCN),
                bth_dt: extract('.demo.DOB[0]', CENDANTUMFCN),
                guest_age: extract('.demo.AGE[0]', CENDANTUMFCN),
                gndr_cd: extract('.demo.GEN[0]', CENDANTUMFCN),
                mrtl_sts: extract('.demo.MST[0]', CENDANTUMFCN),
                pri_lang_cd: extract('.demo.PLN[0]', CENDANTUMFCN),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: ''
            };
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_addr",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let recordsB = CENDANTUMFCN.filter(checkBusiness => {
            let parent = extract('.badd[0]',checkBusiness)
            if(!parent){
                return false;
            }else{
                return true;
            }
        }).map(stayGuestB =>{
            let badd = extract('.badd[0]',stayGuestB)
            let cnid = extract('.cnid[0]',stayGuestB)
            return{
                stay_file_cnsmr_id: extract('.CSQ[0]', cnid),
                bus_nm: extract('.BNM[0]', badd),
                addr_typ: 'B',
                addr_ln_1: extract('.AD1[0]', badd),
                addr_ln_2: extract('.AD2[0]', badd),
                addr_ln_3: extract('.AD3[0]', badd),
                addr_ln_4: extract('.AD4[0]', badd),
                cty_nm: extract('.CIN[0]', badd),
                st_cd: extract('.STC[0]', badd),
                post_cd: extract('.ZIP[0]', badd),
                cntry_cd: extract('.CON[0]', badd),
                lat: extract('.LAT[0]', badd),
                long: extract('.LON[0]', badd),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: ''

            }

        })
        let recordsH =  CENDANTUMFCN.filter(checkHome => {
            let parent = extract('.radd[0]',checkHome)
            if(!parent){
                return false;
            }else{
                return true;
            }
        }).map(stayGuestB =>{
            let radd = extract('.radd[0]',stayGuestB)
            let cnid = extract('.cnid[0]',stayGuestB)
                return{
                    stay_file_cnsmr_id: extract('.CSQ[0]', cnid),
                    bus_nm: extract('.BNM[0]', radd),
                    addr_typ: 'H',
                    addr_ln_1: extract('.AD1[0]', radd),
                    addr_ln_2: extract('.AD2[0]', radd),
                    addr_ln_3: extract('.AD3[0]', radd),
                    addr_ln_4: extract('.AD4[0]', radd),
                    cty_nm: extract('.CIN[0]', radd),
                    st_cd: extract('.STC[0]', radd),
                    post_cd: extract('.ZIP[0]', radd),
                    cntry_cd: extract('.CON[0]', radd),
                    lat: extract('.LAT[0]', radd),
                    long: extract('.LON[0]', radd),
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
                }
        })
        let record = recordsH.concat(recordsB);
        return record;
    }
},
{
    tableName: "stg_stay_email",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let data_src_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json);
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let records = CENDANTUMFCN.map((CENDANTUMFCN,CENDANTUMFCN_Index) => {
            if (extract('.eadd.AD1[0]',CENDANTUMFCN).length !== 0){
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN),
                    data_src_id: data_src_id,
                    email_addr_txt: extract('.eadd.AD1[0]', CENDANTUMFCN),
                    email_typ_cd:'',
                    email_sts_cd:'',
                    email_sts_dt:'',
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
    
                };

            } else {
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN),
                    data_src_id: data_src_id,
                    email_addr_txt: '',
                    email_typ_cd:'',
                    email_sts_cd:'',
                    email_sts_dt:'',
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
    
                };

            }
            
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_evnt",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFEV = extract('..CENDANTUMFEV',json);

        let records = CENDANTUMFEV.map((CENDANTUMFEV,CENDANTUMFEV_Index) => {
            return {
                stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFEV), //stay_file_cnsmr_id,
                book_ref: extract('.eref.BRF[0]', CENDANTUMFEV),
                evnt_ref: extract('.eref.ERF[0]', CENDANTUMFEV),
                evnt_sts_cd: extract('.evstat.ESC[0]', CENDANTUMFEV),
                evnt_sts_dt: extract('.evstat.ESD[0]', CENDANTUMFEV),
                bus_unt_cd: extract('.evsite.BSU[0]', CENDANTUMFEV),
                brand_id: extract('.evsite.BRD[0]', CENDANTUMFEV),
                site_id: extract('.evsite.SIT[0]', CENDANTUMFEV),
                evnt_strt_dt: extract('.evdate.ESD[0]', CENDANTUMFEV),
                evnt_end_dt: extract('.evdate.ECD[0]', CENDANTUMFEV),
                evnt_ld_dy: extract('.evdate.ELD[0]', CENDANTUMFEV),
                evnt_dur_dy: extract('.evdate.EDD[0]', CENDANTUMFEV),
                book_orig_dt: extract('.evsrc.ORD[0]', CENDANTUMFEV),
                book_orig_typ_cd: extract('.evsrc.OTC[0]', CENDANTUMFEV),
                book_orig_sys_nm: extract('.evsrc.OSY[0]', CENDANTUMFEV),
                book_orig_cd: extract('.evsrc.ORC[0]', CENDANTUMFEV),
                corp_cli_id: extract('.client.CRP[0]', CENDANTUMFEV),
                agent_id: extract('.client.AGN[0]', CENDANTUMFEV),
                mbr_cd: extract('.client.MBC[0]', CENDANTUMFEV),
                mbr_num: extract('.client.MBI[0]', CENDANTUMFEV),
                mkt_seg_cd: extract('.mkdat.MKS[0]', CENDANTUMFEV),
                orig_prmt_cd: extract('.mkdat.OPC[0]', CENDANTUMFEV),
                actl_prmt_cd: extract('.mkdat.APC[0]', CENDANTUMFEV),
                pty_tot_cnt: extract('.mkdat.NIP[0]', CENDANTUMFEV),
                adult_tot_cnt: extract('.mkdat.NOA[0]', CENDANTUMFEV),
                chld_tot_cnt: extract('.mkdat.NOC[0]', CENDANTUMFEV),
                chld_pres_ind: extract('.mkdat.POC[0]', CENDANTUMFEV),
                cmdy_cd: extract('.trcomm.CMC[0]', CENDANTUMFEV),
                actl_num_of_rms: extract('.trcomm.SQT[0]',CENDANTUMFEV),
                actl_psrv_amt: extract('.trcomm.SVL[0]', CENDANTUMFEV),
                orig_rt_pln_cd: extract('.mkdat.ORP[0]', CENDANTUMFEV),
                actl_rt_pln_cd: extract('.mkdat.ARP[0]', CENDANTUMFEV),
                prdct_cls_cd: extract('.mkdat.PCC[0]', CENDANTUMFEV),
                pmt_meth_cd: extract('.pymnt.PYM[0]', CENDANTUMFEV),
                pmt_ref: extract('.pymnt.PYR[0]', CENDANTUMFEV),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: '',
                chkin_tm: extract('.evdate.CIT[0]', CENDANTUMFEV),
				chkout_tm: extract('.evdate.COT[0]', CENDANTUMFEV),
				chkin_clrk_id: extract('.evdate.CII[0]', CENDANTUMFEV),
				chkin_clrk_nm: extract('.evdate.CIM[0]', CENDANTUMFEV),
				chkout_clrk_id: extract('.evdate.COI[0]', CENDANTUMFEV),
				chkout_clrk_nm: extract('.evdate.COM[0]', CENDANTUMFEV),
                shr_wth_ind:extract('.eref.SHR[0]',CENDANTUMFEV),
				pms_corp_acct_nm:extract('.client.CRN[0]',CENDANTUMFEV),
				trvl_agnt_nm:extract('.client.TAN[0]',CENDANTUMFEV),
				wyndhm_drct_num:extract('.client.WDN[0]',CENDANTUMFEV),
				wyndhm_drct_ponbr:extract('.client.WDP[0]',CENDANTUMFEV),
				wyndhm_drct_cmpny_nm:extract('.client.WDC[0]',CENDANTUMFEV),

            };
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_trans",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFTR = extract('..CENDANTUMFTR',json);

        let records = CENDANTUMFTR.map((CENDANTUMFTR,CENDANTUMFTR_Index) => {
            return {
                stay_file_cnsmr_id:extract('.cnid.CSQ[0]', CENDANTUMFTR), //stay_file_cnsmr_id,
                CENDANTUMFTR_seq_no: String(CENDANTUMFTR_Index + 1),
                book_ref: extract('.tref.BRF[0]', CENDANTUMFTR),
                evnt_ref: extract('.tref.ERF[0]', CENDANTUMFTR),
                sls_ref_num: extract('.tref.TSR[0]', CENDANTUMFTR),
                trans_dt: extract('.time.TRD[0]', CENDANTUMFTR),
                trans_tm: extract('.time.TRT[0]', CENDANTUMFTR),
                cmdy_cd: extract('.trcomm.CMC[0]', CENDANTUMFTR),
                prdct_cls_cd: extract('.trcomm.PCC[0]', CENDANTUMFTR),
                quot_qty: extract('.trcomm.QQT[0]', CENDANTUMFTR),
                quot_amt: extract('.trcomm.QVL[0]', CENDANTUMFTR),
                sls_qty: extract('.trcomm.SQT[0]', CENDANTUMFTR),
                sls_amt: extract('.trcomm.SVL[0]', CENDANTUMFTR),
                rm_num: extract('.trcomm.RMN[0]', CENDANTUMFTR),
                crncy_cd: extract('.curr.CID[0]', CENDANTUMFTR),
                crncy_rt: extract('.curr.CCF[0]', CENDANTUMFTR),
                actl_prmt_cd: extract('.mkdat.APC[0]', CENDANTUMFTR),
                actl_rt_pln_cd: extract('.mkdat.ARP[0]', CENDANTUMFTR),
                orig_rt_pln: extract('.mkdat.ORP[0]', CENDANTUMFTR),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: '',
                hskp_prsn_nm: extract('.trcomm.HKN[0]', CENDANTUMFTR),
                rm_typ_dscr:extract('.trcomm.RTD[0]',CENDANTUMFTR),
				rt_pln_nm:extract('.mkdat.RPN[0]',CENDANTUMFTR),

            };
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_guest_num_seg",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let records = CENDANTUMFCN.map((CENDANTUMFCN,CENDANTUMFCN_Index) => {
            return extract('.cnnum',CENDANTUMFCN).map((ccnumloop,ccnumloop_Index) => {
                let nty_val =  extract('.NTY[0]', ccnumloop);
                let trimmedNKD =  extract('.NKD[0]', ccnumloop);
                let segcd = '';
                if (nty_val != "PH") {
                    segcd = trimmedNKD;
                }
                else if (trimmedNKD === "HOME") {
                    segcd = "HM";
                }
                else if (trimmedNKD === "BUSI") {
                    segcd = "WK";
                }
                else if (trimmedNKD === "OTHER") {
                    segcd = "WK";
                }
                else if (trimmedNKD === "FAX") {
                    segcd = "FX";
                }
                else {
                    segcd = trimmedNKD;
                }
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN), //stay_file_cnsmr_id,
                    CENDANTUMFCN_Seqno: String(CENDANTUMFCN_Index + 1),
                    cnnum_Seqno: String(ccnumloop_Index + 1),
                    num_seg_typ: nty_val,
                    num_seg_cd: segcd,
                    num_seg_val: extract('.NVL[0]', ccnumloop),
                    mbr_enrl_dt: extract('.NED[0]', ccnumloop),
                    num_exp_dt: extract('.NEX[0]', ccnumloop),
                    affinity_crd_ind: extract('.IAC[0]', ccnumloop),
                    num_seg_sts: extract('.CNS[0]', ccnumloop),
                    num_seg_sts_dt: extract('.CND[0]', ccnumloop),
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
                };
            })
            
        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_guest_atrb",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let records = CENDANTUMFCN.map((CENDANTUMFCN,CENDANTUMFCN_Index) => {
            return extract('.cnatt',CENDANTUMFCN).map((cnattLoop,cnattLoop_Index) => {
                if (extract('.',cnattLoop).length !== 0){
                
                    return {
                        stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN), //stay_file_cnsmr_id,
                        cnattLoop_Index: String(cnattLoop_Index + 1),
                        atrb_typ_cd: extract('.ATY[0]', cnattLoop),
                        atrb_typ_desc: extract('.AVL[0]', cnattLoop),
                        atrb_dt: extract('.ADT[0]', cnattLoop),
                        btch_num: '',
                        data_src_nm: data_src_nm,
                        src_file_nm: s3outputPath,
                        cur_rec_ind: 'Y',
                        job_sess_id: '',
                        job_run_id: '',
                        create_usr: 'PmsStay_lambda',
                        create_ts: '',
                        usr_cmnt: ''
        
                    };
                } else {
                    return {
                        stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN), //stay_file_cnsmr_id,
                        cnattLoop_Index: String(cnattLoop_Index + 1),
                        atrb_typ_cd: '',
                        atrb_typ_desc: '',
                        atrb_dt: '',
                        btch_num: '',
                        data_src_nm: data_src_nm,
                        src_file_nm: s3outputPath,
                        cur_rec_ind: 'Y',
                        job_sess_id: '',
                        job_run_id: '',
                        create_usr: 'PmsStay_lambda',
                        create_ts: '',
                        usr_cmnt: ''
        
                    };
    
                }
                
                
    
            });
        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_brand_rstrc",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let data_src_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json);
        let stay_file_cnsmr_id = extract('..CENDANTUMFCN.cnid.CSQ[0]', json)
        let CENDANTUMFCN = extract('..CENDANTUMFCN',json);

        let records = CENDANTUMFCN.map((CENDANTUMFCN,CENDANTUMFCN_Index) => {
            if (extract('.blrest',CENDANTUMFCN).length !== 0){
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN), //stay_file_cnsmr_id,
                    CENDANTUMFCN_Index: String(CENDANTUMFCN_Index + 1),
                    data_src_id: data_src_id,
                    bus_unt_cd: extract('.blrest.BSU[0]', CENDANTUMFCN),
                    brand_id: extract('.blrest.BRD[0]', CENDANTUMFCN),
                    rstrc_typ: extract('.blrest.RTY[0]', CENDANTUMFCN),
                    rstrc_lvl: extract('.blrest.RLV[0]', CENDANTUMFCN),
                    opt_in_dt: '',
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
    
                };

            } else {
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', CENDANTUMFCN), //stay_file_cnsmr_id,
                    CENDANTUMFCN_Index: String(CENDANTUMFCN_Index + 1),
                    data_src_id: data_src_id,
                    bus_unt_cd: '',
                    brand_id: '',
                    rstrc_typ: '',
                    rstrc_lvl: '',
                    opt_in_dt: '',
                    btch_num: '',
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
    
                };

            }
            
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_jrnl",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let CENDANTUMFJR = extract('..CENDANTUMFJR',json);

        let records = CENDANTUMFJR.map((CENDANTUMFJR,CENDANTUMFJR_Index) => {
            return {
                stay_file_cnsmr_id: '',
                CENDANTUMFJR_Index: String(CENDANTUMFJR_Index + 1),
                jrnl_acct_typ: extract('.acty.SCC[0]', CENDANTUMFJR),
                jrnl_dt: extract('.jrdt.JDT[0]', CENDANTUMFJR),
                jrnl_tm: extract('.jrdt.JTM[0]', CENDANTUMFJR),
                evnt_ref_num: extract('.foln.FLN[0]', CENDANTUMFJR),
                rm_num: extract('.rmnr.RMN[0]', CENDANTUMFJR),
                frst_nm: extract('.jrname.JFN[0]', CENDANTUMFJR),
                mid_nm: extract('.jrname.JMN[0]', CENDANTUMFJR),
                lst_nm: extract('.jrname.JLN[0]', CENDANTUMFJR),
                trans_cd: extract('.trnc.TRC[0]', CENDANTUMFJR),
                trans_desc: extract('.trnc.TDS[0]', CENDANTUMFJR),
                trans_amt: extract('.trnc.AMT[0]', CENDANTUMFJR),
                cmdy_cd: extract('.jrcomm.CMC[0]', CENDANTUMFJR),
                adj_rsn_cd: extract('.adjr.ARC[0]', CENDANTUMFJR),
                adj_rsn_desc: extract('.adjr.CMT[0]', CENDANTUMFJR),
                adj_allow_flg: extract('.adjr.ALW[0]', CENDANTUMFJR),
                jrnl_amt: extract('.adam.AAM[0]', CENDANTUMFJR),
                pmt_amt: extract('.adam.PAY[0]', CENDANTUMFJR),
                chrg_amt: extract('.adam.CHG[0]', CENDANTUMFJR),
                clrk_init: extract('.init.INI[0]', CENDANTUMFJR),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: ''


            };
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_bus_src",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let data_src_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json);
        let pms_vrsn = extract('..CENDANTUMFBH.recv.PMV[0]',json);
        let pms_typ_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let curr_cd = extract('..CENDANTUMFBH.dsrc.CUR[0]',json);
        let site_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json).substr(2);
        let brand_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json).substr(0, 2);
        let bus_dt = extract('..CENDANTUMFBH.hdate.DTF[0]',json);

        let tot_cnsmr_rcrds = extract('..CENDANTUMFBH.ctot.TCR[0]',json);
        let tot_evnt_rcrds = extract('..CENDANTUMFBH.ctot.TER[0]',json);
        let tot_tran_rcrds = extract('..CENDANTUMFBH.ctot.TTR[0]',json);
        let tot_tran_rev = extract('..CENDANTUMFBH.ctot.TTD[0]',json);
        let tot_jrnl_rcrds = extract('..CENDANTUMFBH.ctot.TJR[0]',json);



        let stay_file_cnsmr_id = extract('..CENDANTUMFBH', json)
        let stayguest = extract('..CENDANTUMFBH.tsrc',json);

        let records = stayguest.map((stayguest,stayguest_Index) => {
            if (extract('.[0]',stayguest).length !== 0){
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', stay_file_cnsmr_id), //stay_file_cnsmr_id,
                    stayguest_Index: String(stayguest_Index + 1),
                    data_src_id: data_src_id,
                    site_id: site_id,
                    brand_id:brand_id,
                    bus_dt: bus_dt,
                    bus_src_cd: extract('.SBC[0]', stayguest),
                    bus_src_desc: extract('.SDP[0]', stayguest),
                    rm_sld_cnt: extract('.NRS[0]', stayguest),
                    rm_rvnu_tot: extract('.RRV[0]', stayguest),
                    rm_rvnu_adj: extract('.RAJ[0]', stayguest),
                    btch_num: '',
                    pms_vrsn: pms_vrsn,
                    pms_typ_nm: pms_typ_nm,
                    curr_cd: curr_cd,
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    tot_cnsmr_rcrds:tot_cnsmr_rcrds,
                    tot_evnt_rcrds:tot_evnt_rcrds,
                    tot_tran_rcrds:tot_tran_rcrds,
                    tot_tran_rev:tot_tran_rev,
                    tot_jrnl_rcrds:tot_jrnl_rcrds,
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
                    
                };

            }else{
                return {
                    stay_file_cnsmr_id: extract('.cnid.CSQ[0]', stay_file_cnsmr_id), //stay_file_cnsmr_id,
                    stayguest_Index: String(stayguest_Index + 1),
                    site_id: '',
                    brand_id:'',
                    bus_dt: '',
                    bus_src_cd: '',
                    bus_src_desc: '',
                    rm_sld_cnt: '',
                    rm_rvnu_tot: '',
                    rm_rvnu_adj:  '',
                    btch_num: '',
                    pms_vrsn: pms_vrsn,
                    pms_typ_nm: pms_typ_nm,
                    curr_cd: curr_cd,
                    data_src_nm: data_src_nm,
                    src_file_nm: s3outputPath,
                    cur_rec_ind: 'Y',
                    job_sess_id: '',
                    tot_cnsmr_rcrds:tot_cnsmr_rcrds,
                    tot_evnt_rcrds:tot_evnt_rcrds,
                    tot_tran_rcrds:tot_tran_rcrds,
                    tot_tran_rev:tot_tran_rev,
                    tot_jrnl_rcrds:tot_jrnl_rcrds,
                    job_run_id: '',
                    create_usr: 'PmsStay_lambda',
                    create_ts: '',
                    usr_cmnt: ''
                    
                };

            }
            
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_stay_sls_occp",
    transform: function(json,s3outputPath,seqNum) {
        let data_src_nm = extract('..CENDANTUMFBH.recv.PMT[0]',json);
        let site_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json).substr(2);
        let brand_id = extract('..CENDANTUMFBH.dsrc.SRC[0]',json).substr(0, 2);
        let curr_cd_val = extract('..CENDANTUMFBH.dsrc.CUR[0]',json)
        let stayGuest = extract('..CENDANTUMFBH',json);

        let records = stayGuest.map((stayGuest, stayguest_index ) => {
            return {
                stay_file_cnsmr_id: '',
                stayguest_index : String(stayguest_index  + 1),
                site_id: site_id,
                brand_id:brand_id,
                bus_dt: extract('.hdate.DTF[0]', stayGuest),
                rm_sld_cnt: extract('.ttot.TRS[0]', stayGuest),
                dy_use_rm_sld_cnt: extract('.ttot.DUS[0]', stayGuest),
                cmplmt_rm_sld_cnt: extract('.ttot.CRS[0]', stayGuest),
                rm_rvnu_tot_amt: extract('.ttot.TRR[0]', stayGuest),
                rm_rvnu_adj_amt: extract('.ttot.TAR[0]', stayGuest),
                rm_at_prop_cnt: extract('.ttot.RMS[0]', stayGuest),
                rm_off_mkt_cnt: extract('.ttot.ROM[0]', stayGuest),
                crs_orig_rsrv_no_show_cnt: extract('.ttot.NSH[0]', stayGuest),
                prop_orig_rsrv_no_show_cnt : extract('.ttot.NSP[0]', stayGuest),
                crs_orig_rsrv_canc_cnt: extract('.ttot.COR[0]', stayGuest),
                prop_orig_rsrv_canc_cnt: extract('.ttot.CPR[0]', stayGuest),
                trn_dn : extract('.ttot.TRN[0]', stayGuest),
                fns_occp_rm_cnt: extract('.ttot.TFO[0]', stayGuest),
                fns_incl_rm_occp_pct: extract('.ttot.OWF[0]', stayGuest),
                fns_excl_rm_occp_pct: extract('.ttot.ONF[0]', stayGuest),
                fns_excl_rm_sld_cnt : extract('.ttot.TNF[0]', stayGuest),
                fns_incl_avg_dly_rt: extract('.ttot.AWF[0]', stayGuest),
                fns_excl_avg_dly_rt : extract('.ttot.ANF[0]', stayGuest),
                fns_rm_reim : extract('.ttot.FRA[0]', stayGuest),
                rm_rvnu_adj_allow_amt : extract('.ttot.TAA[0]', stayGuest),
                rm_rvnu_adj_disall_amt: extract('.ttot.TAD[0]', stayGuest),
                fix_ind: extract('.ttot.FFG[0]', stayGuest),
                reltv_rm_rt_ind: extract('.ttot.RRR[0]', stayGuest),
                pms_typ_cd: extract('.recv.PMT[0]', stayGuest),
                pms_ver: extract('.recv.PMV[0]', stayGuest),
                dy_use_pd_rm_cnt : extract('.ttot.DRP[0]', stayGuest),
                dy_use_unpd_rm_cnt : extract('.ttot.DRU[0]', stayGuest),
                dy_use_rm_rvnu : extract('.ttot.DRR[0]', stayGuest),
                no_show_pd_rm_cnt: extract('.ttot.NRP[0]', stayGuest),
                no_show_unpd_rm_cnt: extract('.ttot.NRU[0]', stayGuest),
                no_show_rm_rvnu   : extract('.ttot.NRR[0]', stayGuest),
                othr_pd_rm_cnt : extract('.ttot.ORP[0]', stayGuest),
                othr_rm_rvnu : extract('.ttot.ORR[0]', stayGuest),
                in_house_pd_rm_cnt: extract('.ttot.IRP[0]', stayGuest),
                in_house_rm_rvnu: extract('.ttot.IRR[0]', stayGuest),
                tot_rm_pd_cnt: extract('.ttot.TPR[0]', stayGuest),
                btch_num: '',
                data_src_nm: data_src_nm,
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                usr_cmnt: '',
                zero_rt_rms: extract('.ttot.ZRM[0]', stayGuest),
                loylty_adr: extract('.ttot.LDR[0]', stayGuest),
                adr_wth_fns: extract('.ttot.AWC[0]', stayGuest),
                adr_wthout_fns: extract('.ttot.ANC[0]', stayGuest),
                avg_rev_per_rm: extract('.ttot.ARR[0]', stayGuest),
                rev_per_avlbl_rm: extract('.ttot.RAR[0]', stayGuest),
                tot_trnsctn: extract('.ttot.TRM[0]', stayGuest),
                tot_walkins: extract('.ttot.TWA[0]', stayGuest),
                tot_no_show: extract('.ttot.TNS[0]', stayGuest),
                tot_rsrv_cxcl: extract('.ttot.TCN[0]', stayGuest),
                tot_early_chkins: extract('.ttot.TEI[0]', stayGuest),
                tot_erly_chkouts: extract('.ttot.TEO[0]', stayGuest),
                tot_txble_rm_rev: extract('.ttot.TXR[0]', stayGuest),
                tot_non_txble_rm_rev: extract('.ttot.TNR[0]', stayGuest),
                tot_partly_txble_rm_rev: extract('.ttot.PRR[0]', stayGuest),
                tot_food_bvrg_rev: extract('.ttot.FBR[0]', stayGuest),
                tot_tax_rev: extract('.ttot.TTX[0]', stayGuest),
                tot_othr_rev: extract('.ttot.TOR[0]', stayGuest),
                tot_rev: extract('.ttot.TGR[0]', stayGuest),
                tot_csh_paymnts: extract('.ttot.TCA[0]', stayGuest),
                tot_crdt_crd_paymnts: extract('.ttot.TCC[0]', stayGuest),
                tot_paymnts: extract('.ttot.TTP[0]', stayGuest),
                tot_drct_bill_trnsfrs: extract('.ttot.TDB[0]', stayGuest),
                curr_cd: curr_cd_val,
                out_of_invntry_rms: extract('.ttot.OOI[0]', stayGuest),
                out_of_ordr_rms: extract('.ttot.OOO[0]', stayGuest),
                fns_rm_reim_wthout_tx_Amt: extract('.ttot.FRX[0]', stayGuest),

                
            };
            

        });
        return records;
        

        
    }
},
{
    tableName: "stg_btch_hdr",
    transform: function(json,s3outputPath,seqNum) {
        let CENDANTUMFBH = extract('..CENDANTUMFBH',json);

        let records = CENDANTUMFBH.map((CENDANTUMFBH,CENDANTUMFBH_Index) => {
            return {
                //stay_file_cnsmr_id: stay_file_cnsmr_id,
                CENDANTUMFBH_Index: String(CENDANTUMFBH_Index + 1),
                btch_num: '',
                src_nm: extract('.dsrc.SRC[0]', CENDANTUMFBH),
                btch_create_dt: extract('.hdate.CDT[0]', CENDANTUMFBH),
                btch_create_tm: extract('.hdate.CTM[0]', CENDANTUMFBH),
                dt_from: extract('.hdate.DTF[0]', CENDANTUMFBH),
                dt_thru: extract('.hdate.DTT[0]', CENDANTUMFBH),
                stg_load_strt_ts: '',
                stg_load_cmpl_ts: '',
                std_load_strt_ts: '',
                std_load_cmpl_ts: '',
                mdm_load_strt_ts: '',
                mdm_load_cmpl_ts: '',
                btch_sts: '',
                btch_sts_ts: '',
                btch_stg: '',
                src_file_nm: s3outputPath,
                cur_rec_ind: 'Y',
                job_sess_id: '',
                job_run_id: '',
                create_usr: 'PmsStay_lambda',
                create_ts: '',
                updt_usr: '',
                updt_ts: '',
                usr_cmnt: '',
                rs_btch_sts: ''
                
            };
            

        });
        return records;
        

        
    }
}
];

module.exports = StaytableDefs;