package au.csiro.obr17q.variantspark;

import au.csiro.obr17q.variantspark.CommonFunctions._
import org.junit.Assert._
import org.junit.Test

class CommonFunctionsTest {

	@Test def testGetBmi() {
    assertEquals("The output should be a count of individuals.", 25, getBmi(170, 74), 1)
    assertEquals("The output should be a count of individuals.", 25, getBmi(1.70, 74), 1)
    assertEquals("The output should be a count of individuals.", 37, getBmi(200, 150), 1)
    assertEquals("The output should be a count of individuals.", 37, getBmi(2.00, 150), 1)
    assertEquals("The output should be a count of individuals.", 88, getBmi(130, 150), 1)
    assertEquals("The output should be a count of individuals.", 88, getBmi(1.30, 150), 1)
	}

	@Test def testVariantDist() {
		assertEquals("The output should be a count of individuals.", 0, variantDist("0|0"), 0)
    assertEquals("The output should be a count of individuals.", 0, variantDist("0/0"), 0)
    assertEquals("The output should be a count of individuals.", 1, variantDist("0|1"), 0)
    assertEquals("The output should be a count of individuals.", 1, variantDist("0/1"), 0)
    assertEquals("The output should be a count of individuals.", 2, variantDist("1|1"), 0)
    assertEquals("The output should be a count of individuals.", 2, variantDist("1/1"), 0)
    assertEquals("The output should be a count of individuals.", 0, variantDist(".|."), 0)
    assertEquals("The output should be a count of individuals.", 0, variantDist("./."), 0)
    assertEquals("The output should be a count of individuals.", 0, variantDist("0/1:27:0:1"), 1)
	}

}
