using System.Collections.Generic;

namespace Data_Analytics_Tools.Constants
{
    public static class ApacheConstants
    {
        public static List<string> GetApacheKnownTables()
        {
            List<string> tables = new List<string>()
            {
                "location","lte_l1_dl_tp", "lte_handover_stats", "lte_l1_ul_tp", "lte_pdsch_meas", "lte_sib3_info", "gmm_state", "wcdma_bler", 
                "wcdma_pilot_pollution", "reg_state", "lte_rlc_ul_stats", "events", "vocoder_info", "gsm_server_bcch_rxlev", "log_info", "nb1_ded_info_nas",
                "wcdma_hsdpa_cqi", "wcdma_rx_power", "lte_pucch_tx_info", "wifi_scanned", "lte_emm_state", "gsm_serv_cell_info",
                "wcdma_bearers", "wcdma_to_gsm_ho_dur", "android_info_1sec", "data_wcdma_rlc_stats", "wcdma_hsupa_stats",
                "lte_sib5_interfreq", "gsm_l1_burst_stats", "lte_sib2_info", "pp_statement_sum_wifi_enable", "lte_reselection_stats",
                "wcdma_sib5_params", "lte_cell_meas", "wcdma_sib1_params", "android_radio_params_lte", "gsm_coi_per_chan",
                "android_radio_params_wcdma", "wcdma_idle_cell_info", "device_location", "umts_data_activation_stats", "wcdma_sib7_params", "lte_meas",
                "wcdma_cm_gsm_meas", "wcdma_tx_power", "lte_pusch_tx_info", "polqa_mos", "android_info_2sec", "serving_system", "signalling",
                "lte_cqi", "statement_sum_voice_dial", "pp_statement_sum_answer", "wcdma_cell_meas", "gsm_rlt_counter", "nb1_ml1_gm_dci_info",
                "lte_frame_timing", "lte_rrc_transmode_info", "lte_pdcch_dec_result", "wcdma_hsdpa_serving_psc", "lte_serv_cell_info",
                "statement_sum_answer", "lte_volte_rtp_ho_delay", "wcdma_aset_full_list", "lte_rrc_connection_setup",
                "pp_show_polqa_mos_samples", "lte_pdcp_stats", "lte_rrc_tmsi", "wcdma_rrc_state", "detected_radio_event",
                "gsm_dsc", "pp_statement_sum_voice_dial", "lte_connected_meas_config", "gsm_hopping_list", "gsm_rr_cipher_alg",
                "gladiator", "wifi_active", "lte_mac_ul_tx_stat", "wcdma_rrc_meas_events","activate_dedicated_eps_bearer_context_request_params",
                "voicecall", "android_radio_params_gsm","lte_volte_stats", "lte_mib_info", "gsm_cell_meas", "lte_rrc_state",
                "gsm_tx_meas", "polqa_mos_ori_cont_wav", "lte_neigh_meas", "mm_state", "pp_log_to_operator_map", "azq_report_gen_preprocess_info",
                "gsm_power_scan", "lte_msg1_report", "statement", "wcdma_nset_full_list", "wifi_scanned_info", "lte_tdd_config",
                "wcdma_hsdpa_stats", "lte_nas_esm_plain", "statement_sum_wait", "lte_rlc_stats", "logs", "lte_volte_rtp_msg",
                "detected_radio_voice_call_session", "lte_tx_power", "pp_voice_report", "lte_sib1_info", "lte_attach_stats",
                "unused", "statement_sum_wifi_enable", "spatial_ref_sys", "geometry_columns", "layer_styles", "lte_inst_rsrp_1",
                "lte_inst_rsrq_1","lte_sinr_1", "lte_inst_rssi_1", "lte_earfcn_1", "lte_physical_cell_id_1", "lte_cqi_cw0_1",
                "lte_l1_dl_n_active_carriers", "lte_l1_dl_throughput_all_carriers_mbps", "lte_l1_ul_throughput_all_carriers_mbps_1",
                "lte_volte_rtp_jitter_dl_ms", "wcdma_aset_rscp_avg","wcdma_aset_ecio_avg", "detected_radio_voice_call_setup_duration",
                "wcdma_aset_ecio_1", "wcdma_aset_rscp_1", "wcdma_txagc", "wcdma_n_aset_cells", "wcdma_n_dset_cells",
                "wcdma_n_pilot_polluting_cells", "data_wcdma_rlc_dl_throughput", "data_hsdpa_cqi_number_avg", "wcdma_bler_average_percent_all_channels",
                "data_hsdpa_thoughput","data_hsupa_total_e_dpdch_throughput","gsm_rxlev_sub_dbm", "gsm_rxqual_sub", "gsm_arfcn_bcch",
                "polqa_mos_1", "data_trafficstat_dl_mbps", "data_trafficstat_ul_mbps", "Cm_info_Table", "Voice_Sessions_Table", "Mos_Table",
                "Codec_1", "Codec_Table", "Codec_Table_1","Events_info","Mos_Table_1","Voice_Sessions_Table_1","Cm_info_Table_1",
                "Cm_info_Table_2","Long_Call_Table", "CM_Data_Table","CM_Data_Table_1","Codec_3","Telkom_Codec","Codec_Mos","RAT_Table"
            };
            return tables;
        }
    }
}
