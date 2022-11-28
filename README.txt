DSP-202 Hadoop Map-Reduce  Eitan Tshernihovsky and Tal Marciano

In this project, we designed a Map-Reduce system built on the AWS EMR (student) environment using Hadoop.
The mission was to process big data (Hebrew literature from the Google N-Grams dataset) and calculate the probability P(W3 | W1, W2) for each of the word triplets (W1, W2, W3) we observed in the data.

## How to run:

1. Create an S3 bucket named "map-reduce-dsc202-bucket".
2. After compiling the associated .java files, Upload the following jar files to the bucket(If needed rename accordingly):
    - First-step.jar
    - Second-step.jar
    - Third-step.jar
    - Fourth-step.jar
3. Compile and Run the Main file from the target folder(If needed rename accordingly): java -jar main.jar


Our Map Reduce steps ware:


## Step 1: 
For each triplet such as (W1, W2, W3), we will count the number of times (w1, w2, w3), (w1, w2), (w2, w3), (w1), (w2), (w3) has appeard on the data set.
Each N gram has information on how many times it has appeared in the same year in the data set.
In the Map stage, generate a key value to each of (w1, w2, w3), (w1, w2), (w2, w3), (w1), (w2), (w3) with the value of the N-gram occurrences.
In the Reducer stage, we will sum all of the values accumulated for each key.


## Step 2: 
Creating an array of values as a preparation for the formula calculation step.
In the Map stage, we will move the key value we received from step one to the value side.
For each triplet, we will generate the keys (w2), (w3), (w2, w3), (w1, w2) and send them with the value mentioned above.
For each pair or single word we receive, we will use the same key as before with their value.
In the Reducer step, we will assemble a key-value format when the key is the triplet (taken from the value we received from the Mapper),
and the value will be an array of numbers ordered in this way {c1, n1, c2, n2, n3}.
Each of the keys (singles and pairs) we get from the Mapper will hold in its list of values information from relevant triplets
as well as information on the single's / pair's (in respect) occurrences.
We will create a list for the triplets and save the occurrences of the key itself.
Then we will go through the list of the triplets and create the array and fill in the values we have (we can't fill it all for some of the values are located in another key's list of values).
After creating the array we will send the key value.


## Step 3:
Calculating the probability formula.
In the Map stage, we will send the keys and the arrays as is to the reducers.
There are redundancies in the keys and the reducer will get the list of values for each of the keys.

In the Reducer stage, we will merge the arrays of each key into a single array (each array holds different values and lack the others).
after the merge, we will take the values from it and use them to calculate the probability formula:

P(W3 | W1, W2) = K3 * (N3/C2) + (1-K3) * K2 * (N2/C1) + (1-K3) * (1-K2) * (N1/C0)
			while:
	K2 = (log(N2 + 1) + 1) / (log(N2 + 1) + 2),
	K3 = (log(N3 + 1) + 1) / (log(N3 + 1) + 2)
	N1 is the number of times w3 occurs.
	N2 is the number of times sequence (w2,w3) occurs.
	N3 is the number of times the sequence (w1,w2,w3) occurs.
	C0 is the total number of word instances in the corpus.
	C1 is the number of times w2 occurs.
	C2 is the number of times sequence (w1,w2) occurs

Then, we will send the same key (the triplet) with the probability calculated.


## Step 4:
Sort the key values with w1, and w2 in ascending order, and then with the probability in descending order.
In the Map stage, we will transfer the value to the key so the key will be (w1, w2, w3, p) and the value will remain an empty Text.
The shuffle and sort part will sort the keys and the reducer will write the keywords and probability.


## Statistics:

 --Note!-- We only implemented Local Aggregation, Using Combiner in the first step.
 
  Here are the number of "Reduce input records" as seen in the syslog file:
  
  With Local Aggregation:
   First-step: 4454351
   Second-step: 7565970
   Third-step: 6599596
   Fourth-step: 1650268
   
  Without Local Aggregation:
   First-step: 486084221
   Second-step: 7565970
   Third-step: 6599596
   Fourth-step: 1650268 
   
While the rest of the steps took 2 minutes without Local Aggregation, The first step took 14 minutes WITH Local Aggregation and 40 minutes WITHOUT Local Aggregation(Using 1 Master and 7 Workers)!   


## Analytics:

אבא ואמא שלי	0.046599364925497926
אבא ואמא לא	0.040374486498243684
אבא ואמא היו	0.0397063955060654
אבא ואמא עד	0.02244760753859685
אבא ואמא של	0.018717938467790806

יחסינו עם הערבים	0.13532922059939811
יחסינו עם ארצות	0.0714266758148943
יחסינו עם גרמניה	0.06804166175428597
יחסינו עם העולם	0.04646068834288375
יחסינו עם שכנינו	0.04368093484260121

ויחד עם זה	0.4972206750673872
ויחד עם זאת	0.31686830421957257
ויחד עם כל	0.02269218197152824
ויחד עם בני	0.007899924267806054
ויחד עם כך	0.00782156095416897

ויזכו לראות בנים	0.47713194347172005
ויזכו לראות את	0.22607329577141042
ויזכו לראות נחת	0.21038265110067747

עוד עשרים שנה	0.35928337489897827
עוד עשרים וחמש	0.09759666690470438
עוד עשרים דקות	0.09338818332600794
עוד עשרים וארבע	0.07738970250632467
עוד עשרים אלף	0.051776912458426784

תלו את הקולר	0.16395869223943302
תלו את כל	0.10000280477218823
תלו את המן	0.07004509236564159
תלו את האשמה	0.06411872960800029
תלו את עצמם	0.06122279435231635


שלום עם ישראל	0.1529769858605857
שלום עם מצרים	0.04335349627303296
שלום עם הערבים	0.04052247784354414
שלום עם כל	0.03317031418317473
שלום עם סוריה	0.021998449169210062

עשה מאמץ עליון	0.12136366521522553
עשה מאמץ גדול	0.11007014171842366
עשה מאמץ ניכר	0.09781367318229324
עשה מאמץ מיוחד	0.08972575427494461
עשה מאמץ רב	0.08050005372625292

חיוך של אושר	0.0657176388255766
חיוך של שביעות	0.05696440123493564
חיוך של מבוכה	0.03491949735806675
חיוך של הנאה	0.03058970891658881
חיוך של קורת	0.029253951237749133

חלפו שבועות אחדים	0.39950451308917195
חלפו שבועות וחודשים	0.2491914400304469
חלפו שבועות מספר	0.22127738518597106

מטעם המוסדות הלאומיים	0.48964751345116786
מטעם המוסדות הציוניים	0.18061738337372596
מטעם המוסדות היהודיים	0.1167909597295686
מטעם המוסדות המיישבים	0.10808173152814168



























