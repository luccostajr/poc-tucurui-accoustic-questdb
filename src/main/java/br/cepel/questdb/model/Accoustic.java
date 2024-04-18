package br.cepel.questdb.model;

public class Accoustic {
  private String dsId;
  private Long ts;
  private Double frequency;
  private Double amplitude;

  public Accoustic(String dsId, Long ts, Double frequency, Double amplitude) {
    this.dsId = dsId;
    this.ts = ts;
    this.frequency = frequency;
    this.amplitude = amplitude;
  }

  public String getDsId() {
    return dsId;
  }

  public void setDsId(String dsId) {
    this.dsId = dsId;
  }

  public Long getTs() {
    return ts;
  }

  public void setTs(Long ts) {
    this.ts = ts;
  }

  public Double getFrequency() {
    return frequency;
  }

  public void setFrequency(Double frequency) {
    this.frequency = frequency;
  }

  public Double getAmplitude() {
    return amplitude;
  }

  public void setAmplitude(Double amplitude) {
    this.amplitude = amplitude;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Accoustic accousticBO = (Accoustic) obj;
    return dsId.equals(accousticBO.dsId) && ts.equals(accousticBO.ts) && frequency.equals(accousticBO.frequency)
        && amplitude.equals(accousticBO.amplitude);
  }

  @Override
  public String toString() {
    return "{AccousticBO: {\"dsId\":\"" + dsId + "\", \"ts\":\"" + ts + "\", \"frequency\":\"" + frequency
        + "\", \"amplitude:\"" + amplitude + "\"}}";
  }
}
