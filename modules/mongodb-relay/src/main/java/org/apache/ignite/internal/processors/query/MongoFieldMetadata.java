package org.apache.ignite.internal.processors.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Field descriptor.
 */
public class MongoFieldMetadata implements GridQueryFieldMetadata {
    /** */
    private static final long serialVersionUID = 0L;

    /** Schema name. */
    String schemaName;

    /** Type name. */
    String typeName;

    /** Name. */
    String name;

    /** Type. */
    String type;


    /** Nullability. See {@link java.sql.ResultSetMetaData#isNullable(int)} */
    int nullability;

    /**
     * Required by {@link Externalizable}.
     */
    public MongoFieldMetadata() {
        // No-op
    }

    /**
     * @param schemaName Schema name.
     * @param typeName Type name.
     * @param name Name.
     * @param type Type.
     * @param precision Precision.
     * @param scale Scale.
     */
    MongoFieldMetadata(@Nullable String schemaName, @Nullable String typeName, String name, String type,
        int nullability) {
        assert name != null && type != null : schemaName + " | " + typeName + " | " + name + " | " + type;

        this.schemaName = schemaName;
        this.typeName = typeName;
        this.name = name;
        this.type = type;
        this.nullability = nullability;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public String typeName() {
        return typeName;
    }

    /** {@inheritDoc} */
    @Override public String fieldName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String fieldTypeName() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public int nullability() {
        return nullability;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, schemaName);
        U.writeString(out, typeName);
        U.writeString(out, name);
        U.writeString(out, type);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        schemaName = U.readString(in);
        typeName = U.readString(in);
        name = U.readString(in);
        type = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MongoFieldMetadata.class, this);
    }

	@Override
	public int precision() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int scale() {
		// TODO Auto-generated method stub
		return 0;
	}
}

