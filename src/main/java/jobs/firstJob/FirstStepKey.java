package jobs.firstJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class FirstStepKey implements WritableComparable<FirstStepKey> {
	private Text firstWord;
	private Text secondWord;
	private Text thirdWord;

	public FirstStepKey() {
		this.firstWord = new Text();
		this.secondWord = new Text();
		this.thirdWord = new Text();		
	}

	public FirstStepKey(Text firstWord, Text secondWord, Text thirdWord) {
		this.firstWord = new Text(firstWord.toString());
		this.secondWord = new Text(secondWord.toString());
		this.thirdWord= new Text(thirdWord.toString());		
	}

	public FirstStepKey(String firstWord, String secondWord, String thirdWord) {
		this.firstWord = new Text(firstWord);
		this.secondWord = new Text(secondWord);
		this.thirdWord = new Text(thirdWord);		
	}

	public void setFields(String firstWord, String secondWord, String thirdWord) {
		this.firstWord.set(firstWord);
		this.secondWord.set(secondWord);
		this.thirdWord.set(thirdWord);		
	}

	public FirstStepKey( FirstStepKey otherFirstKey) {
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
		int size = this.getSize();
		Text[] output = new Text[size];
		output[0] = this.getFirstWord();
		if(!this.secondWord.toString().equals("*"))
			output[1] = this.getSecondWord();
		if(!this.thirdWord.toString().equals("*"))
			output[2] = this.getThirdWord();
		return output;	
	}

	public int getSize(){
		int size = 3;
		if(this.secondWord.toString().equals("*"))
			size --;
		if(this.thirdWord.toString().equals("*"))
			size --;
		return size;
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
	
	@Override
	public int compareTo(FirstStepKey otherFirstStepKey) {
		int comp =0;
		//keys with s number of words are bigger then keys with k number of words when s > k.
		if((!this.thirdWord.toString().equals("*") && otherFirstStepKey.getThirdWord().toString().equals("*"))
			|| (!this.secondWord.toString().equals("*") && otherFirstStepKey.getSecondWord().toString().equals("*")))
			return 1;
		if((this.thirdWord.toString().equals("*") && !otherFirstStepKey.getThirdWord().toString().equals("*"))
			|| (this.secondWord.toString().equals("*") && !otherFirstStepKey.getSecondWord().toString().equals("*")))
			return -1;

		//same number of words. Laxicographic-comparence.
		if(comp==0)				
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
		if (!(o instanceof FirstStepKey)) return false;
		FirstStepKey that = (FirstStepKey) o;
		return Objects.equals(getFirstWord(), that.getFirstWord()) &&
				Objects.equals(getSecondWord(), that.getSecondWord()) &&
				Objects.equals(getThirdWord(), that.getThirdWord());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getFirstWord(), getSecondWord(), getThirdWord());
	}

}