package broadcast.join;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class YOrderDetails {
    private String goodsId;//商品id
    private String goodsName;//商品名字
    private Integer count;//数量
    private BigDecimal totalMoney;//总计

    @Override
    public String toString() {
        return "YOrderDetails{" +
                "goodsId='" + goodsId + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", count=" + count +
                ", totalMoney=" + totalMoney +
                '}';
    }
}
