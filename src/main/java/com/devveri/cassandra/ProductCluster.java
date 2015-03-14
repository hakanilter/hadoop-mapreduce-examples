package com.devveri.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * CREATE KEYSPACE product
 *      replication = {
 *      'class' : 'SimpleStrategy',
 *      'replication_factor' : 1
 * };
 *
 * CREATE TABLE product.product_cluster (
 *      product_id int PRIMARY KEY,
 *      title text,
 *      cluster_id text
 * ) WITH compression = {
 *      'sstable_compression' : 'DeflateCompressor'
 * };
 */

@Table(keyspace = "product", name = "product_cluster")
public class ProductCluster {

    @PartitionKey
    @Column(name = "product_id")
    private Integer productId;

    private String title;

    @Column(name = "cluster_id")
    private String clusterId;

    public ProductCluster() {

    }

    public ProductCluster(Integer productId, String title, String clusterId) {
        this.productId = productId;
        this.title = title;
        this.clusterId = clusterId;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductCluster that = (ProductCluster) o;

        if (clusterId != null ? !clusterId.equals(that.clusterId) : that.clusterId != null) return false;
        if (productId != null ? !productId.equals(that.productId) : that.productId != null) return false;
        if (title != null ? !title.equals(that.title) : that.title != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = productId != null ? productId.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (clusterId != null ? clusterId.hashCode() : 0);
        return result;
    }

}
