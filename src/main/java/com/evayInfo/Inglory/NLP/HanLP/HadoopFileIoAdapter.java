package com.evayInfo.Inglory.NLP.HanLP;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.io.IIOAdapter;
import com.hankcs.hanlp.seg.CRF.CRFSegment;
import com.hankcs.hanlp.seg.Segment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 *
 * Created by sunlu on 17/10/26.
 * 参考链接：
 * Spark中使用HanLP分词：http://m.blog.csdn.net/l294265421/article/details/72932042
 *
 */
public class HadoopFileIoAdapter implements IIOAdapter {

    @Override
    public InputStream open(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        return fs.open(new Path(path));
    }

    @Override
    public OutputStream create(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        OutputStream out = fs.create(new Path(path));
        return out;
    }

    private static Segment segment;

    static {
        HanLP.Config.IOAdapter = new HadoopFileIoAdapter();
        segment = new CRFSegment();
    }

}
