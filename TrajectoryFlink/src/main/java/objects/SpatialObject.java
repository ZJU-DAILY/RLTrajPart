package objects;

import java.io.Serializable;

public abstract class SpatialObject extends Object implements Serializable {

    public String objID;
    public long timeStampMillisec;

    public SpatialObject() {}

}

