package com.example.jobplatform.worker;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operator/workers")
public class WorkerController {

    private final WorkerHeartbeatService heartbeatService;
    private final WorkerProperties properties;

    public WorkerController(
            WorkerHeartbeatService heartbeatService, WorkerProperties properties) {
        this.heartbeatService = heartbeatService;
        this.properties = properties;
    }

    @GetMapping
    public List<WorkerLiveness> list() {
        return heartbeatService.list(properties.livenessTimeout());
    }
}
