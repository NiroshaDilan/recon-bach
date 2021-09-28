package lk.icoder.reconbatch.config;

import lk.icoder.reconbatch.model.ReconciliationDto;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

/**
 * @Project recon-batch
 * @Author DILAN on 9/25/2021
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    @StepScope
    public FlatFileItemReader<ReconciliationDto> reconciliationDtoFlatFileItemReader() {

        FileSystemResource fileSystemResource = new FileSystemResource("src/main/resources/data/csv/CEFTS_Reconfile.csv");
        return new FlatFileItemReaderBuilder<ReconciliationDto>()
                .saveState(false)
                .resource(fileSystemResource)
                .delimited()
                .names(new String[]{"masked_pan", "tran_deposition", "tran_type_id",
                        "cur_recon_net", "amt_recon_net", "tstamp_trans", "sys_trace_audit_no",
                        "tstamp_local", "msg_reason_code_acq", "merch_type", "retrieval_ref_no",
                        "approval_code", "card_acpt_term_id", "card_acpt_country",
                        "card_acpt_region", "card_acpt_name_loc", "cvv_cvc_result", "cvv2_cvc2_result",
                        "net_term_id", "net_id_acq", "net_id_iss", "inst_id_iss", "inst_id_acq",
                        "acct_id", "date_recon_iss", "date_recon_acq", "impact_to_iss", "amt_cash_back",
                        "eft_tlv_data", "tran_class", "has_pin", "c32", "c33"})
                .fieldSetMapper(fieldSet -> {
                    ReconciliationDto reconciliationDto = new ReconciliationDto();

                    reconciliationDto.setMasked_pan(fieldSet.readString("masked_pan").trim());
                    reconciliationDto.setTran_deposition(fieldSet.readString("tran_deposition").trim());
                    reconciliationDto.setTran_type_id(fieldSet.readString("tran_type_id").trim());
                    reconciliationDto.setCur_recon_net(fieldSet.readString("cur_recon_net").trim());
                    reconciliationDto.setAmt_recon_net(fieldSet.readString("amt_recon_net").trim());
                    reconciliationDto.setTstamp_trans(fieldSet.readString("tstamp_trans").trim());
                    reconciliationDto.setSys_trace_audit_no(fieldSet.readString("sys_trace_audit_no").trim());
                    reconciliationDto.setTstamp_local(fieldSet.readString("tstamp_local").trim());
                    reconciliationDto.setMsg_reason_code_acq(fieldSet.readString("msg_reason_code_acq").trim());
                    reconciliationDto.setMerch_type(fieldSet.readString("merch_type").trim());
                    reconciliationDto.setRetrieval_ref_no(fieldSet.readString("retrieval_ref_no").trim());
                    reconciliationDto.setApproval_code(fieldSet.readString("approval_code").trim());
                    reconciliationDto.setCard_acpt_term_id(fieldSet.readString("card_acpt_term_id").trim());
                    reconciliationDto.setCard_acpt_country(fieldSet.readString("card_acpt_country").trim());
                    reconciliationDto.setCard_acpt_region(fieldSet.readString("card_acpt_region").trim());
                    reconciliationDto.setCvv_cvc_result(fieldSet.readString("cvv_cvc_result").trim());
                    reconciliationDto.setCard_acpt_name_loc(fieldSet.readString("card_acpt_name_loc").trim());
                    reconciliationDto.setCvv2_cvc2_result(fieldSet.readString("cvv2_cvc2_result").trim());
                    reconciliationDto.setNet_term_id(fieldSet.readString("net_term_id").trim());
                    reconciliationDto.setNet_id_acq(fieldSet.readString("net_id_acq").trim());
                    reconciliationDto.setNet_id_iss(fieldSet.readString("net_id_iss").trim());
                    reconciliationDto.setInst_id_acq(fieldSet.readString("inst_id_acq").trim());
                    reconciliationDto.setInst_id_iss(fieldSet.readString("inst_id_iss").trim());
                    reconciliationDto.setAcct_id(fieldSet.readString("acct_id").trim());
                    reconciliationDto.setDate_recon_iss(fieldSet.readString("date_recon_iss").trim());
                    reconciliationDto.setDate_recon_acq(fieldSet.readString("date_recon_acq").trim());
                    reconciliationDto.setImpact_to_iss(fieldSet.readString("impact_to_iss").trim());
                    reconciliationDto.setAmt_cash_back(fieldSet.readString("amt_cash_back").trim());
                    reconciliationDto.setEft_tlv_data(fieldSet.readString("eft_tlv_data").trim());
                    reconciliationDto.setTran_class(fieldSet.readString("tran_class").trim());
                    reconciliationDto.setHas_pin(fieldSet.readString("has_pin").trim());
                    reconciliationDto.setC32(fieldSet.readString("c32").trim());
                    reconciliationDto.setC33(fieldSet.readString("c33").trim());

                    return reconciliationDto;
                })
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<ReconciliationDto> reconciliationDtoJdbcBatchItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<ReconciliationDto>()
                .dataSource(dataSource)
                .beanMapped()
                .sql("INSERT INTO RECONCILIATION (masked_pan, tran_deposition, tran_type_id," +
                        "cur_recon_net, amt_recon_net, tstamp_trans, sys_trace_audit_no, " +
                        "tstamp_local, msg_reason_code_acq, merch_type, retrieval_ref_no," +
                        "approval_code, card_acpt_term_id, card_acpt_country, card_acpt_region," +
                        "card_acpt_name_loc, cvv_cvc_result, cvv2_cvc2_result, net_term_id," +
                        "net_id_acq, net_id_iss, inst_id_iss, inst_id_acq, acct_id, date_recon_iss," +
                        "date_recon_acq, impact_to_iss, amt_cash_back, eft_tlv_data," +
                        "tran_class, has_pin, c32, c33)" +
                        " VALUES (:masked_pan, :tran_deposition, :tran_type_id," +
                        ":cur_recon_net, :amt_recon_net, :tstamp_trans, :sys_trace_audit_no, " +
                        ":tstamp_local, :msg_reason_code_acq, :merch_type, :retrieval_ref_no," +
                        ":approval_code, :card_acpt_term_id, :card_acpt_country, :card_acpt_region," +
                        ":card_acpt_name_loc, :cvv_cvc_result, :cvv2_cvc2_result, :net_term_id," +
                        ":net_id_acq, :net_id_iss, :inst_id_iss, :inst_id_acq, :acct_id, :date_recon_iss," +
                        ":date_recon_acq, :impact_to_iss, :amt_cash_back, :eft_tlv_data," +
                        ":tran_class, :has_pin, :c32, :c33)")
                .build();
    }

    @Bean
    public Step reconciliationStep() throws Exception {

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.afterPropertiesSet();

        return this.stepBuilderFactory
                .get("reconciliation-step1")
                .<ReconciliationDto, ReconciliationDto>chunk(100)
                .reader(reconciliationDtoFlatFileItemReader())
                .writer(reconciliationDtoJdbcBatchItemWriter(null))
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job reconciliationWritingJob() throws Exception {
        return this.jobBuilderFactory
                .get("reconciliationJob")
                .incrementer(new RunIdIncrementer())
                .start(reconciliationStep())
                .build();
    }
}
