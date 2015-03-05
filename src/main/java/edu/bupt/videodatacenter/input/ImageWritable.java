/**    
* @Title: ImageWritable.java  
* @Package edu.bupt.videodatacenter.input  
* @author xmpy xiaomengzhaopy_gmail_com   
* @date 2014-5-15 下午2:17:18  
* @version V1.0    
*/
package edu.bupt.videodatacenter.input;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * <p>
 * 实现Writable接口的图像类，里面使用BufferedImage来存储图像信息
 * </p>
 * 
 * @author xmpy xiaomengzhaopy_gmail_com
 * @date 2014-4-1 下午10:04:08
 * 
 */
public class ImageWritable implements Writable {

	private BufferedImage bufferedImage;

	public ImageWritable(BufferedImage b) {
		bufferedImage = b;
	}

	/**
	 * <p>
	 * 返回该对象中存储的图像
	 * </p>
	 * 
	 * @param
	 * @return BufferedImage 返回类型
	 * @throws
	 */
	public BufferedImage getBufferedImage() {
		return bufferedImage;
	}

	/*
	 * (non-Javadoc) <p>Title: readFields</p> <p>从二进制流中读取ImageWritable</p>
	 * 
	 * @param in
	 * 
	 * @throws IOException
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		int len = WritableUtils.readVInt(in);
		byte[] temp = new byte[len];
		in.readFully(temp, 0, len);
		ByteArrayInputStream byteStream = new ByteArrayInputStream(temp); // 输入流；
		bufferedImage = ImageIO.read(byteStream); // 从输入流中，读取图片存入image中，而这里in可以为ByteArrayInputStream();
	}

	/*
	 * (non-Javadoc) <p>Title: write</p> <p>序列化ImageWritable</p>
	 * 
	 * @param out
	 * 
	 * @throws IOException
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {

	}

}

