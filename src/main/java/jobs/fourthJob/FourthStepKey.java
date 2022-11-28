package jobs.fourthJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class FourthStepKey implements WritableComparable<FourthStepKey> {
	private Text key;
	

	public 
	FourthStepKey() {
		this.key = new Text();		
	}

	public FourthStepKey(Text key) {
		this.key = new Text(key.toString());
	}

	public FourthStepKey(String key) {
		this.key = new Text(key);		
	}

	public void setFields(String key) {
		this.key.set(key);		
	}

	public FourthStepKey( FourthStepKey otherFirstKey) {
		this.key = otherFirstKey.getKey();	
	}

	public Text getKey() {
		return this.key;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) key).readFields(in) ;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable) key).write(out) ;
	}
	
	public int compareTo(FourthStepKey otherFourthStepKey) {
		String[] k1 = ((Text) key).toString().split("\t"), k2= ((Text) otherFourthStepKey.getKey()).toString().split("\t");
			String[] words1 = k1[0].split(" ");
			String[] words2 = k2[0].split(" ");
			int comp =words1[0].compareTo(words2[0]);
			if(comp==0)
				comp =words1[1].compareTo(words2[1]);
			if(comp==0)
				comp = Double.parseDouble(k1[1])> Double.parseDouble(k2[1])? -1 : 1; // descending order (0.6<0.5)
			return comp;
	}

	public String toString() {
		return this.key.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof FourthStepKey)) return false;
		FourthStepKey that = (FourthStepKey) o;
		return Objects.equals(getKey(), that.getKey());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getKey());
	}

	// public int getCode() {
	// 	return firstWord.hashCode() + decade.hashCode();
	// }
}
