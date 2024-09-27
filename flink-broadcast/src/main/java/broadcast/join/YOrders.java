package broadcast.join;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
//订单明细类(订单id,商品id,订单数量)
public class YOrders {
    private String itemId;
    private String goodsId;
    private Integer count;


    @Override
    public String toString() {
        return "YOrders{" +
                "itemId='" + itemId + '\'' +
                ", goodsId='" + goodsId + '\'' +
                ", count=" + count +
                '}';
    }
}
