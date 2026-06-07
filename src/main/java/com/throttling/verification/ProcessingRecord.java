package com.throttling.verification;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processing_record")
public class ProcessingRecord extends PanacheEntityBase {

    @Id
    @Column(name = "idempotency_key")
    public String idempotencyKey;

    @Column(name = "processed_at")
    public Instant processedAt;
}
