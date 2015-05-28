package pigUDF;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;

public class TupleConv extends EvalFunc<DataBag> {

	DataBag db = BagFactory.getInstance().newDefaultBag();
	@Override
	public DataBag exec(Tuple input) throws IOException {
		
		List<Object> li = input.getAll();
		for (int i = 0 ; i < li.size(); i++) {
			TupleFactory tf = TupleFactory.getInstance();
			Tuple tupleVar = tf.newTuple();
			tupleVar.append(li.get(i));
			db.add(tupleVar);
		}
		return db;
	}

	
}
