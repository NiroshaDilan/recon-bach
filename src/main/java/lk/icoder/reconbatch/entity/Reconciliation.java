package lk.icoder.reconbatch.entity;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

/**
 * @Project recon-batch
 * @Author DILAN on 9/25/2021
 */
@Entity
@Table
@Getter
@Setter
@RequiredArgsConstructor
public class Reconciliation {

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO)
   private Long id;
   private String masked_pan;
   private String tran_deposition;
   private String tran_type_id;
   private String cur_recon_net;
   private String amt_recon_net;
   private String tstamp_trans;
   private String sys_trace_audit_no;
   private String tstamp_local;
   private String msg_reason_code_acq;
   private String merch_type;
   private String retrieval_ref_no;
   private String approval_code;
   private String card_acpt_term_id;
   private String card_acpt_country;
   private String card_acpt_region;
   private String card_acpt_name_loc;
   private String cvv_cvc_result;
   private String cvv2_cvc2_result;
   private String net_term_id;
   private String net_id_acq;
   private String net_id_iss;
   private String inst_id_iss;
   private String inst_id_acq;
   private String acct_id;
   private String date_recon_iss;
   private String date_recon_acq;
   private String impact_to_iss;
   private String amt_cash_back;
   private String eft_tlv_data;
   private String tran_class;
   private String has_pin;
   private String c32;
   private String c33;

}
