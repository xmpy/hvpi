/**    
* @Title: VideoInputFormat.java  
* @Package edu.bupt.videodatacenter.input  
* @author xmpy xiaomengzhaopy_gmail_com   
* @date 2014-5-15 下午2:17:03  
* @version V1.0    
*/
package edu.bupt.videodatacenter.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**  
 * <p>实现使用hadoop分析视频文件，该类将视频文件解析成帧数和帧图像的键值对</p>
 * @author xmpy xiaomengzhaopy_gmail_com  
 * @date 2014-4-1 下午10:37:48  
 *    
 */
public class VideoInputFormat extends FileInputFormat<Text, ImageWritable>{

	/* (non-Javadoc)
	* <p>Title: createRecordReader</p>  
	* <p>Description: </p>  
	* @param arg0
	* @param arg1
	* @return
	* @throws IOException
	* @throws InterruptedException  
	* @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)  
	*/
	@Override
	public RecordReader<Text, ImageWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new  VideoRecordReader();
	}
	
	//确保一个视频一个mapper处理
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
