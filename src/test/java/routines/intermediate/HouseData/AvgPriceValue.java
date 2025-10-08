package routines.intermediate.HouseData;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgPriceValue implements Writable {
    private float price;
    private int n;

    public AvgPriceValue() {
    }

    public AvgPriceValue(float price, int n) {
        this.price = price;
        this.n = n;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.n);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.price = dataInput.readFloat();
        this.n = dataInput.readInt();
    }
}
