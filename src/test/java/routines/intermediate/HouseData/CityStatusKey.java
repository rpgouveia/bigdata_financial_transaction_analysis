package routines.intermediate.HouseData;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityStatusKey implements WritableComparable<CityStatusKey> {
    private String city;
    private String status;

    public CityStatusKey() {
    }

    public CityStatusKey(String city, String status) {
        this.city = city;
        this.status = status;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public int compareTo(CityStatusKey o) {
        int r = this.city.compareTo(o.getCity());
        if(r != 0){
            return r;
        }
        return this.status.compareTo(o.getStatus());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.city);
        dataOutput.writeUTF(this.status);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.city = dataInput.readUTF();
        this.status = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "city = " + city + ", status = " + status;
    }
}
