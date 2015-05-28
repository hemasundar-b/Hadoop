package pigUDF;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import java.io.IOException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;



public class DateFunc extends EvalFunc<Tuple> {
	
	DateTimeFormatter inputDtFormatter = DateTimeFormat.forPattern("MM/dd/yy HH:mm");
	
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		TupleFactory tf = TupleFactory.getInstance();
		Tuple outTuple = tf.newTuple();
		String inputDate = input.get(0).toString();
		DateTime dt = inputDtFormatter.parseDateTime(inputDate);
		String dtVar = dt.toString("YYYYMMDD");
		String timeVar =  dt.toString("HH");
		outTuple.append(dtVar);
		outTuple.append(timeVar);
		return outTuple;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema outSchema = new Schema();
		outSchema.add(new FieldSchema("crimedate", DataType.CHARARRAY));
		outSchema.add(new FieldSchema("crimeTime", DataType.CHARARRAY));
		return outSchema;
	}

	
}
