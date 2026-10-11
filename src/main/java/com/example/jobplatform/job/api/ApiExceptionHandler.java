package com.example.jobplatform.job.api;

import com.example.jobplatform.job.IdempotencyConflictException;
import com.example.jobplatform.job.DeadLetterReplayConflictException;
import com.example.jobplatform.job.InvalidSubmissionException;
import com.example.jobplatform.job.CancellationConflictException;
import com.example.jobplatform.job.JobNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ApiExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    ResponseEntity<ProblemDetail> notFound(JobNotFoundException exception, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "Job not found", exception.getMessage(), request);
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    ResponseEntity<ProblemDetail> conflict(
            IdempotencyConflictException exception, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Idempotency conflict", exception.getMessage(), request);
    }

    @ExceptionHandler(DeadLetterReplayConflictException.class)
    ResponseEntity<ProblemDetail> replayConflict(
            DeadLetterReplayConflictException exception, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Replay conflict", exception.getMessage(), request);
    }

    @ExceptionHandler(CancellationConflictException.class)
    ResponseEntity<ProblemDetail> cancellationConflict(
            CancellationConflictException exception, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Cancellation conflict", exception.getMessage(), request);
    }

    @ExceptionHandler({InvalidSubmissionException.class, MissingRequestHeaderException.class})
    ResponseEntity<ProblemDetail> badRequest(Exception exception, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", exception.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> validation(
            MethodArgumentNotValidException exception, HttpServletRequest request) {
        ProblemDetail detail = baseProblem(
                HttpStatus.BAD_REQUEST,
                "Request validation failed",
                "One or more request fields are invalid",
                request);
        Map<String, String> violations = new LinkedHashMap<>();
        exception.getBindingResult().getFieldErrors().forEach(error ->
                violations.putIfAbsent(error.getField(), error.getDefaultMessage()));
        detail.setProperty("violations", violations);
        return ResponseEntity.badRequest().body(detail);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ProblemDetail> unreadable(
            HttpMessageNotReadableException exception, HttpServletRequest request) {
        return problem(
                HttpStatus.BAD_REQUEST,
                "Malformed JSON",
                "Request body could not be parsed",
                request);
    }

    private ResponseEntity<ProblemDetail> problem(
            HttpStatus status, String title, String detail, HttpServletRequest request) {
        return ResponseEntity.status(status).body(baseProblem(status, title, detail, request));
    }

    private ProblemDetail baseProblem(
            HttpStatus status, String title, String detail, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create("https://job-platform.local/problems/" + status.value()));
        problem.setInstance(URI.create(request.getRequestURI()));
        return problem;
    }
}
