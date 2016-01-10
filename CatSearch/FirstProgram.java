import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;
import hipi.util.ByteUtils;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import javax.imageio.ImageIO;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.BytesWritable;



public class FirstProgram extends Configured implements Tool {
	  
	public static class FirstProgramMapper extends Mapper<ImageHeader, BytesWritable, IntWritable, Text> {
	    public void map(ImageHeader key, BytesWritable value, Context context) 
	      throws IOException, InterruptedException {
	    	
            if (value != null) {
                ByteArrayInputStream byedata = new ByteArrayInputStream(value.getBytes());
                String imgHash = "";
                try {
                    BufferedImage picture = ImageIO.read(byedata);         
                    ImagePHash p = new ImagePHash(); 
                    imgHash = p.getHash(picture);

                } catch (Exception e) {
                    //e.printStackTrace();
                }
    	        // Emit record to reducer
                String hashval = ByteUtils.asHex(value.getBytes());
                Text hushStr = new Text();
                hushStr.set("1:" + imgHash + ":" + hashval + "\n");
    	        context.write(new IntWritable(1), hushStr);

	        } // If (value != null...
	        
	      } // map()
	}
	 
    public static class FirstProgramMapperTwo extends Mapper<ImageHeader, BytesWritable, IntWritable, Text> {
        public void map(ImageHeader key, BytesWritable value, Context context) 
          throws IOException, InterruptedException {
             
            if (value != null) {
                ByteArrayInputStream byedata = new ByteArrayInputStream(value.getBytes());
                BufferedImage picture = ImageIO.read(byedata);
                ImagePHash p = new ImagePHash();
                String imgHash = "";
                try {
                    imgHash = p.getHash(picture);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Emit record to reducer
                String hashval = ByteUtils.asHex(value.getBytes());
                Text hushStr = new Text();
                hushStr.set("2:" + imgHash + ":" + hashval + "\n");
                context.write(new IntWritable(2), hushStr);

            } // If (value != null...
            
          } // map()
    }

	public static class FirstProgramReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	// Initialize a counter and iterate over IntWritable/FloatImage records from mapper
	        int total = 0;
            String result ="";
	        for (Text val : values) {
                result += val.toString();
                total++;
            }

	        if (total > 0) {
	          context.write(null, new Text(result));
	        }

	      } // reduce()

	}
	
	public int run(String[] args) throws Exception {
	    // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        job.setJarByClass(FirstProgram.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),JpegFromHibInputFormat.class,FirstProgramMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),JpegFromHibInputFormat.class,FirstProgramMapperTwo.class);
        job.setReducerClass(FirstProgramReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
	  }
	  
      public static Map sortByValue(Map unsortMap) {     
        List list = new LinkedList(unsortMap.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o1)).getValue())
                            .compareTo(((Map.Entry) (o2)).getValue());
            }
        });
        Map sortedMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
      }

	  public static void main(String[] args) throws Exception {
        // Check input arguments
        if (args.length < 2) {
          System.out.println("Usage: firstprog <input HIB> <output directory>");
          System.exit(0);
        }
	    ToolRunner.run(new FirstProgram(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        ArrayList<String> keyOneLi = new ArrayList<>();
        ArrayList<String> keyTwoLi = new ArrayList<>();
        ArrayList<String> hashOneLi = new ArrayList<>();
        try{
            Path path = new Path(args[2]+"/part-r-00000");
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while(line != null){
                String[] tmp = line.split(":");
                if(tmp.length > 2) {
                    if(tmp[0].equals("1") && !tmp[1].isEmpty()) {
                        keyOneLi.add(tmp[1]);
                        hashOneLi.add(tmp[2]);
                    }
                    else
                        keyTwoLi.add(tmp[1]);
                }
                line = br.readLine();
            }
            br.close();
        } catch(Exception e){
            System.out.println("File not found");
        }
        
        try{
            Path path = new Path(args[2]+"/result");
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));

            ImagePHash p = new ImagePHash();
            Map<Integer, Integer> resultindex = new HashMap<>();
            for(String phashvalue:keyTwoLi)
            {
            	for(Integer i=0;i<keyOneLi.size();i++){
            		Integer temp = p.distance(phashvalue, keyOneLi.get(i));
                    if(resultindex.containsKey(i)){
            		  resultindex.put(i, resultindex.get(i)+temp);
                    } else {
                      resultindex.put(i, temp);
                    }

            	}
            }
            int count = 0;
            Map<Integer, Integer> sortedResult = sortByValue(resultindex);
            for(Map.Entry<Integer, Integer> entry:sortedResult.entrySet()) {
            	if ( count > 9) break;
            	String imagefind = hashOneLi.get(entry.getKey());       
                bw.write(imagefind+" : "+entry.getValue()+"\n");
                count++;
            }
            bw.close();
        } catch(Exception e){
        }

	    System.exit(0);
	  }
	  
	}
