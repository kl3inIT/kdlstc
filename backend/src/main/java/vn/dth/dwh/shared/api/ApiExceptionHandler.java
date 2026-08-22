package vn.dth.dwh.shared.api;

import jakarta.validation.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import vn.dth.dwh.pipeline.application.PipelineRunNotFoundException;
import vn.dth.dwh.pipeline.infrastructure.airflow.AirflowClientException;

@RestControllerAdvice(basePackages = "vn.dth.dwh")
public class ApiExceptionHandler {

    @ExceptionHandler(PipelineRunNotFoundException.class)
    ProblemDetail notFound(PipelineRunNotFoundException exception) {
        return problem(HttpStatus.NOT_FOUND, "Không tìm thấy lượt chạy", exception.getMessage());
    }

    @ExceptionHandler(AirflowClientException.class)
    ProblemDetail airflow(AirflowClientException exception) {
        return problem(HttpStatus.BAD_GATEWAY, "Airflow không sẵn sàng", exception.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalidRequest(MethodArgumentNotValidException exception) {
        String detail = exception.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(error -> error.getDefaultMessage())
                .orElse("Request không hợp lệ");
        return problem(HttpStatus.BAD_REQUEST, "Request không hợp lệ", detail);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail constraintViolation(ConstraintViolationException exception) {
        return problem(HttpStatus.BAD_REQUEST, "Request không hợp lệ", exception.getMessage());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    ProblemDetail conflict(DataIntegrityViolationException exception) {
        return problem(HttpStatus.CONFLICT, "Xung đột dữ liệu", "Dữ liệu đã được ghi bởi request khác");
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        return problem;
    }
}
