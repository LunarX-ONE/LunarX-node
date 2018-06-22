/** LCG(Lunarion Consultant Group) Confidential
 * LCG NoSQL database team is funded by LCG
 * 
 * @author DADA database team 
  * The contents of this file are subject to the Lunarion Public License Version 1.0
  * ("License"); You may not use this file except in compliance with the License
  * The Original Code is:  Lunar NoSQL Database source code 
  * The Lunar NoSQL Database source code is based on Lunarion Cloud Platform(solution.lunarion.com)
  * The Initial Developer of the Original Code is the development team at Lunarion.com.
  * Portions created by lunarion are Copyright (C) lunarion.
  * All Rights Reserved.
  *******************************************************************************
 * 
 */


package LCG.FSystem.Manifold;

import java.io.IOException;

import LCG.FSystem.AtomicStructure.BlockSimple;
import LCG.FSystem.CopyOnWrite.VFileHandler;

public abstract interface SubManifoldInterface {

	abstract public int appendData(int[] records, int from, int end) throws IOException;
	abstract public boolean appendData(int record) throws IOException;
	abstract public VFileHandler getHandler();
}
