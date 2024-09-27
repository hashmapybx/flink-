package broadcast.hive;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HPeople implements Serializable {
    private static final long serialVersionUID = 1L;

    private int id;
    private String name;
}
