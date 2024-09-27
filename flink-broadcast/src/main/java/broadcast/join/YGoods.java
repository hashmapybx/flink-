package broadcast.join;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

//商品类(商品id,商品名字,商品价格)

@Data
@AllArgsConstructor

public class YGoods implements Serializable {
    private static final long serialVersionUID = 1L;
    private String goodsId;
    private String goodsName;
    private BigDecimal goodsPrice;

    @Override
    public String toString() {
        return "YGoods{" +
                "goodsId='" + goodsId + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", goodsPrice=" + goodsPrice +
                '}';
    }
}
