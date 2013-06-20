package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class ReferencedDimension extends BaseDimension {
  private final List<TableReference> references;

  public ReferencedDimension(FieldSchema column, TableReference reference) {
    super(column);
    this.references = new ArrayList<TableReference>();
    this.references.add(reference);
  }

  public ReferencedDimension(FieldSchema column, Collection<TableReference> references) {
    super(column);
    this.references = new ArrayList<TableReference>();
    this.references.addAll(references);
  }

  public void addReference(TableReference reference) {
    references.add(reference);
  }

  public List<TableReference> getReferences() {
    return references;
  }

  public boolean removeReference(TableReference ref) {
    return references.remove(ref);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimensionSrcReferenceKey(getName()),
        MetastoreUtil.getDimensionDestReference(references));
  }

  public ReferencedDimension(String name, Map<String, String> props) {
    super(name, props);
    String refListStr = props.get(MetastoreUtil.getDimensionSrcReferenceKey(getName()));
    String refListDims[] = StringUtils.split(refListStr, ",");
    this.references = new ArrayList<TableReference>(refListDims.length);
    for (String refDimRaw : refListDims) {
      references.add(new TableReference(refDimRaw));
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getReferences() == null) ? 0 :
        getReferences().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ReferencedDimension other = (ReferencedDimension) obj;
    if (this.getReferences() == null) {
      if (other.getReferences() != null) {
        return false;
      }
    } else if (!this.getReferences().equals(other.getReferences())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "references:" + getReferences();
    return str;
  }
}
