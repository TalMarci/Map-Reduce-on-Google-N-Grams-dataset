package jobs.thirdJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class ThirdStepKey implements WritableComparable<ThirdStepKey> {
	private Text firstWord;
	private Text secondWord;
	private Text thirdWord;

	public ThirdStepKey() {
		this.firstWord = new Text();
		this.secondWord = new Text();
		this.thirdWord = new Text();		
	}

	public ThirdStepKey(Text firstWord, Text secondWord, Text thirdWord) {
		this.firstWord = new Text(firstWord.toString());
		this.secondWord = new Text(secondWord.toString());
		this.thirdWord= new Text(thirdWord.toString());		
	}

	public ThirdStepKey(String firstWord, String secondWord, String thirdWord) {
		this.firstWord = new Text(firstWord);
		this.secondWord = new Text(secondWord);
		this.thirdWord = new Text(thirdWord);		
	}

	public void setFields(String firstWord, String secondWord, String thirdWord) {
		this.firstWord.set(firstWord);
		this.secondWord.set(secondWord);
		this.thirdWord.set(thirdWord);		
	}

	public ThirdStepKey( ThirdStepKey otherFirstKey) {
		this.firstWord = otherFirstKey.getFirstWord();
		this.secondWord = otherFirstKey.getSecondWord();
		this.thirdWord = otherFirstKey.getThirdWord();		
	}

	public Text getFirstWord() {
		return this.firstWord;
	}

	public Text getSecondWord() {
		return this.secondWord;
	}

	public Text getThirdWord() {
		return this.thirdWord;
	}

	public Text[] toTextArray(){
		Text[] output = new Text[3];
		output[0] = this.getFirstWord();
		output[1] = this.getSecondWord();
		output[2] = this.getThirdWord();
		return output;	
	}

	

	@Override
	public void readFields(DataInput in) throws IOException {
		((Writable) firstWord).readFields(in) ;
		((Writable) secondWord).readFields(in) ;
		((Writable) thirdWord).readFields(in) ;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable) firstWord).write(out) ;
		((Writable) secondWord).write(out) ;
		((Writable) thirdWord).write(out) ;

	}
	
	
	public int compareTo(ThirdStepKey otherFirstStepKey) {
		int comp =0;
		comp = this.firstWord.toString().compareTo(otherFirstStepKey.getFirstWord().toString());
		if(comp == 0)
			comp = this.secondWord.toString().compareTo(otherFirstStepKey.getSecondWord().toString());
		if(comp == 0)
			comp = this.thirdWord.toString().compareTo(otherFirstStepKey.getThirdWord().toString());									
		return comp;
	}



	public String toString() {
		return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.thirdWord.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof ThirdStepKey)) return false;
		ThirdStepKey that = (ThirdStepKey) o;
		return Objects.equals(getFirstWord(), that.getFirstWord()) &&
				Objects.equals(getSecondWord(), that.getSecondWord()) &&
				Objects.equals(getThirdWord(), that.getThirdWord());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getFirstWord(), getSecondWord(), getThirdWord());
	}

	// public int getCode() {
	// 	return firstWord.hashCode() + decade.hashCode();
	// }
}