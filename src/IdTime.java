import org.apache.hadoop.io.Writable;
import java.io.*;
import java.lang.*;


public class IdTime implements Writable {
  public Long Id;
  public String myTime;

  public IdTime(Long Id, String myTime) {
    this.Id = Id;
    this.myTime = myTime;
  }

  public IdTime() {
    Id = new Long(0);
    myTime = new String("");
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(Id);
    out.writeUTF(myTime);
  }

  public void readFields(DataInput in) throws IOException {
    Id = in.readLong();
    /*
    char[] tmp = new char[4];
    for (int i = 0; i < 4; i++) 
	tmp[i] = in.readChar();
    myTime = new String(tmp);
    */
    myTime = in.readUTF();
  }

  public String toString() {
    return Id.toString() + ":"
        + myTime;
  }

}
