package kafka.model;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class DelayMessage {

  private String message;

  private LocalDateTime time;
}
