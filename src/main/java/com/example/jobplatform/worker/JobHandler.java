package com.example.jobplatform.worker;

public interface JobHandler {

    String jobType();

    void execute(JobExecutionContext context);
}
