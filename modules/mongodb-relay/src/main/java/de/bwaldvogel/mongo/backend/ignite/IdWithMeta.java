package de.bwaldvogel.mongo.backend.ignite;

import de.bwaldvogel.mongo.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

public final class IdWithMeta implements Comparable<IdWithMeta>{

    public static Comparator scoreComparator(){
        return (Object a,Object b)->{
            if(a instanceof IdWithMeta){
                if(b instanceof IdWithMeta) {
                    return -((IdWithMeta) a).compareTo((IdWithMeta) b);
                }
                return -1;
            }
            else{
                return 1;
            }
        };
    }

    final Object key;
    final Document meta;

    public IdWithMeta(Object key, Document meta) {
        this.key = key;
        this.meta = meta;
    }

    public Object getKey() {
        return key;
    }

    
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[key=" + key + ", meta=" + meta.toString() + "]";
    }
    
    public Document meta() {
        return meta;
    }

	@Override
	public int hashCode() {		
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass()) {
			if(key.equals(obj)) {
				return true;
			}
			return false;
		}
		IdWithMeta other = (IdWithMeta) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}
    @Override
    public int compareTo(@NotNull IdWithMeta other) {
        if(this.meta==null) return -1;
        if(other.meta==null) return 1;
        Float score = (Float)meta.getOrDefault("searchScore",0.0f);
        Float vScore = (Float)meta.getOrDefault("vectorSearchScore",0.0f);
        Float score2 = (Float)other.meta.getOrDefault("searchScore",0.0f);
        Float vScore2 = (Float)other.meta.getOrDefault("vectorSearchScore",0.0f);
        float r = (score + vScore) - (score2 + vScore2);
        if(r>0) return 1;
        if(r<0) return -1;
        return 0;
    }

    public boolean mergeWith(IdWithMeta other){
        if(meta!=null){
            if(other.meta!=null){
                Float score = (Float)meta.get("searchScore");
                Float vScore = (Float)meta.get("vectorSearchScore");
                this.meta.putAll(other.meta);
                if(score!=null){
                    this.meta.computeIfPresent("searchScore",(k,v)-> (Float)v+score );
                }
                if(vScore!=null){
                    this.meta.computeIfPresent("vectorSearchScore",(k,v)-> (Float)v+vScore );
                }
                return true;
            }
        }
        return false;
    }
}