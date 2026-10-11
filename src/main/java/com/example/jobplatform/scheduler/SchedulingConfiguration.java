package com.example.jobplatform.scheduler;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SchedulingProperties.class)
public class SchedulingConfiguration {}
