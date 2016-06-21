package ru.yandex.clickhouse.response;

import java.io.IOException;
import java.io.InputStream;

/**
 * что мы тут делаем.
 * на вход приезжает поток байт и разделитель
 * на выходе выезжает некоторое количество байтовых массивов,
 * которые в оригинальном потоке были разделены разделителем
 *
 * делается это путем чтения из потока данных в буфер и выдачи их из буфера.
 * @author orantius
 * @version $Id$
 * @since 7/16/12
 */
public class StreamSplitter {
    private static final int buflen = 65536;

    // начальные параметры
    private final InputStream delegate;
    private final byte sep;

    private byte[] buf;
    // позиция, до которой buf заполнен прочтенным из delegate
    private int posRead;
    // позиция, до которой из buf уже отдано наружу фрагментов через next()
    private int posNext;

    // флаг который символизирует, что из потока надо прочитать один раз всю длни буфера, и больше читать не надо.
    private boolean readOnce;


    public StreamSplitter(ByteFragment bf, byte sep) {
        this.delegate = bf.asStream();
        this.sep = sep;
        buf  = new byte[bf.getLen()];
        readOnce = true;
    }

    public StreamSplitter(InputStream delegate, byte sep, int buflen) {
        this.delegate = delegate;
        this.sep = sep;
        buf  = new byte[buflen];
    }

    public StreamSplitter(InputStream delegate, byte sep) {
        this(delegate,sep, buflen);
    }

    public ByteFragment next() throws IOException {
        // если заслали наружу все что прочитали из потока
        if (posNext >= posRead) {
            // надо прочитать из потока еще
            int readBytes = readFromStream();
            if(readBytes <= 0) {
                // если все отдали, и из потока больше не читается - то не отдаем ничего
                return null;
            }
        }
        // ищем в прочитанном разделитель
        int positionSep;
        while((positionSep = indexOf(buf, sep, posNext, posRead)) < posNext) {
            // пока не нашли разделитель надо прочитать из потока еще
            int readBytes = readFromStream();
            if(readBytes <= 0) {
                // если уже ничего не читается - отдаем весь хвост как результат.
                positionSep = posRead;
                break;
                /*int fragmentStart = posNext;
                posNext = positionSep+1;
                System.out.println("return "+(positionSep-fragmentStart)+" bytes as next() ");
                return new ByteFragment(buf, fragmentStart, positionSep-fragmentStart); */
            }
        }
        // если нашли разделитель - отдаем кусок.
        int fragmentStart = posNext;
        posNext = positionSep+1;
        // System.out.println("return "+(positionSep-fragmentStart)+" bytes as next() ");
        return new ByteFragment(buf, fragmentStart, positionSep-fragmentStart);
    }

    // если в прочитанном но не отправленном куске данных нет разделителя - читаем из потока еще данных
    protected int readFromStream() throws IOException {
        if (readOnce) {
            if (posRead >= buf.length) {
                return -1;
            } else {
                int read = delegate.read(buf, posRead, buf.length - posRead);
                if(read > 0)
                    posRead += read;
                return read;
            }
        } else {
            if (posRead >= buf.length) { // буфер закончился
                shiftOrResize();
            }
            // если буфер не заполнен до конца
            int read = delegate.read(buf, posRead, buf.length - posRead);
            //System.out.println("read "+read+" bytes  from stream");
            if(read > 0)
                posRead += read;
            return read;
        }
    }

    // если поток дочитали до конца буфера - надо создать новый буфер и передвинуть данные на уже отправленную величину.
    // если отправленных данных нет, а буфер все равно дочитан до конца - надо увеличить размер буфера.
    private void shiftOrResize() {
        if(posNext > 0) {
            // System.out.println("shift "+posNext+" bytes");
            byte[] oldBuf = buf;
            buf = new byte[buf.length];
            System.arraycopy(oldBuf, posNext, buf, 0, oldBuf.length-posNext);
            posRead -= posNext;
            posNext = 0;
        } else {
            //System.out.println("double size");
            byte[] oldBuf = buf;
            buf = new byte[buf.length*2];
            System.arraycopy(oldBuf, 0, buf, 0, oldBuf.length);
        }
    }

    private static int indexOf(byte[] array, byte target, int start, int end) {
         for (int i = start; i < end; i++) {
           if (array[i] == target) {
             return i;
           }
         }
         return -1;
     }

    public void close() throws IOException {
        delegate.close();
    }
}
