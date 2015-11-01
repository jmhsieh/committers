package org.jmhsieh.committers;

import java.util.Collections;
import java.util.Map;

/**
 * Class that represents a committer
 */
public class Committer {

  private String name;
  private String hair;
  private String beard;
  private Map<String, String> jiras;

  /**
   *
   * @param name
   * @param hair
   * @param beard
   * @param jiras
   */

  public Committer(String name, String hair, String beard, Map<String, String> jiras) {
    assert jiras != null;
    this.name = name;
    this.hair = hair;
    this.beard = beard;
    this.jiras = jiras;
  }

  public String getName() {
    return name;
  }

  public String getHair() {
    return hair;
  }

  public String getBeard() {
    return beard;
  }

  public Map<String, String> getJiras() {
    return Collections.unmodifiableMap(jiras);
  }

  public void setName(String name) {
    this.name = name;
    }

  public void setHair(String hair) {
    this.hair = hair;
  }

  public void setBeard(String beard) {
    this.beard = beard;
  }

  public void addJiras(String title, String description) {
    jiras.put(title, description);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof Committer)) {
      return false;
    }

      Committer c  = (Committer) o;
    if (this.getName() != c.getName() && this.getName() != null && !this.getName().equals(c.getName()))
      return false;
    if (this.getBeard() != c.getBeard() && this.getBeard() != null && !this.getBeard().equals(c.getBeard()))
      return false;
    if (this.getHair() != c.getHair() && this.getHair() != null && !this.getHair().equals(c.getHair()))
      return false;
    // ignore commits for now

    return true;
  }

}
