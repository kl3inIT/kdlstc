package vn.dth.dwh.pipeline.infrastructure.airflow;

public class AirflowClientException extends RuntimeException {

    public AirflowClientException(String message) {
        super(message);
    }

    public AirflowClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
