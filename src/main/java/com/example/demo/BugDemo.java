package com.example.demo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List ;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class BugDemo 
{

    /* Allocator */
    private RootAllocator allocator ; 

    /* Schema and Vectors */
    private Schema schema ; 
    private VectorSchemaRoot vectorSchemaRoot ; 


    /* Writers */
    private ListWriter listWriter ;
    private StructWriter structWriter; 
    private IntWriter intWriter ;
    private VarCharWriter varCharWriter ;


    public void run() {
        /* Initialize */
        initSchemaAndVectors() ;
        initWriters();

        /* Write some data and export */
        writeData() ;
        System.out.println(vectorSchemaRoot.contentToTSVString());
        export(); // Error thrown in this function
        
        /* Close all */
        closeAll();
    }

    public void initSchemaAndVectors() {

        allocator = new RootAllocator();
        List<Field> fields = Arrays.asList(
            new Field("list_field", new FieldType(true, new ArrowType.List(), null), Arrays.asList(
                new Field("struct_field", new FieldType(true, new ArrowType.Struct(), null), Arrays.asList(
                    new Field("int_field", new FieldType(true, new ArrowType.Int(64, false), null), null),
                    new Field("var_char_field", new FieldType(true, new ArrowType.Utf8(), null), null)
                ))
            ))
        );

        schema = new Schema(fields) ; 
        vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);

    }

    public void initWriters() {

        listWriter = ((ListVector) vectorSchemaRoot.getVector("list_field")).getWriter() ;
        structWriter = listWriter.struct() ; 
        intWriter  = structWriter.integer("int_field");
        varCharWriter = structWriter.varChar("var_char_field");
    } 

    public void writeData() {
        /**
         * Write the following data 
         * Row 0 : [ {0, "value_0"} ]
         * Row 1 : [ {1, "value_1"} ]
         * Row 2 : [ {2, "value_2"} ]
         */

        Object[][] rows = new Object[][] {
            {0, "value_0"}, 
            {1, "value_2"},
            {2, "value_3"}
        };

        for (int i = 0 ; i < rows.length; i++) {
            listWriter.startList();
                structWriter.start();
                    intWriter.writeInt((int) rows[i][0]);
                    writeString(varCharWriter, (String) rows[i][1]);
                structWriter.end();
            listWriter.endList();
        }
        vectorSchemaRoot.setRowCount(rows.length);
    }

    public void export() {
        try (
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator)
        ) {

            Data.exportSchema(allocator, schema, null, arrowSchema);
            Data.exportVectorSchemaRoot(allocator, vectorSchemaRoot, null, arrowArray);
            /* Consume the exported data in some other language by passing memory address. */
        }
    }

    public void closeAll() {
        vectorSchemaRoot.close();
        allocator.close();

    }


    /* Private methods */
    private void writeString(VarCharWriter writer, String value) {

        ArrowBuf buffer = allocator.buffer(256); 
        byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8) ;
        buffer.writeBytes(stringBytes);
        writer.writeVarChar(0, stringBytes.length, buffer);
        buffer.close();
    }


    public static void main( String[] args )
    {
        BugDemo bugDemo = new BugDemo();
        bugDemo.run();
    }
}
