package au.csiro.obr17q.variantspark;

import org.junit.Assert._;
import org.junit.Test;

class PopulationMapTest {

	@Test	def test1000GenomeLikeData() {
    val CSV = Iterator(
        "sample\tpop\tsuper_pop\tgender",
        "HG00096\ta1\tEUR\tmale",
        "HG00097\ta1\tEUR\tfemale",
        "HG00098\ta2\tEUR\tfemale",
        "HG00099\ta2\tEUR\tfemale",
        "HG00100\tb1\tEUR\tfemale",
        "HG00101\tb2\tEUR\tmale",
        "HG00102\tb2\tEUR\tfemale",
        "HG00102\tc1\tEUR\tfemale",
        "HG00102\tc2\tEUR\tfemale"
    )

		val popMap = new PopulationMap(CSV, "sample", '\t', 0, 1)

		assertEquals("The output should be a count of individuals.", 9, popMap.returnMap().length)
    assertEquals("The output should be a count of individuals who belong to a1.", 2, popMap.returnMap(IncludeGroups = Array("a1")).length)
    assertEquals("The output should be a count of individuals who belong to a1 or a2.", 4, popMap.returnMap(IncludeGroups = Array("a1", "a2")).length)
    assertEquals("The output should be a count of individuals who belong to a1 or a2.", 4, popMap.returnMap(IncludeGroups = Array("a1", "a2"), ExcludeGroups = Array("")).length)
    assertEquals("The output should be a count of individuals who belong to a1 or a2.", 4, popMap.returnMap(IncludeGroups = Array("a1", "a2"), ExcludeGroups = Array("b1", "b2")).length)
    assertEquals("The output should be a count of individuals who belong to a1 or a2.", 4, popMap.returnMap(IncludeGroups = Array("a1", "a2", "c1"), ExcludeGroups = Array("c1")).length)
    assertEquals("The output should be a count of individuals who belong to a1 or a2 or c1.", 5, popMap.returnMap(IncludeGroups = Array("a1", "a2", "c1")).length)
    assertEquals("The output should be a count of individuals who do not belong to b1.", 8, popMap.returnMap(ExcludeGroups = Array("b1")).length)
    assertEquals("The output should be a count of individuals who do not belong to b1 or b2.", 6, popMap.returnMap(ExcludeGroups = Array("b1", "b2")).length)
	}
  
}
