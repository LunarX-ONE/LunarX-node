package LCG.FSystem;

public class ReadMe {
	static String readMe = 
			"# ------------------------------------------------------------------------- #\r\n"
			+ "# LunarDB version 1.01                                                      #\r\n"
			+ "# (c) support@lunarion.com, 2015                                            #\r\n"
			+ "# ------------------------------------------------------------------------- #\r\n"
			+ "\r\n"
			+ "Author:\r\n"
			+ "feiben@lunarion.com \r\n"
			+ "neo.carmack@lunarion.com \r\n "
			+ "at solution.lunarion.com \r\n "
			+ "\r\n "
			+ "The file system is one of the core components of " 
			+ "the whole database. It stores the big table for quick "
			+ "search with compressed volumes. To computer scientists or engineers, "
			+ "the source code may looks confusing because of some " 
			+ "fundamental math terms used to describe objects within "
			+ "the system." 
			+ "\r\n "
			+ "\r\n" 
			+ "During the days that i was designing and programming the database "
			+ "(named lunarbase or lunarDB), "
			+ "i have been working on a small topic in algebraic geometry in parallel, "
			+ "using the algebraic cohomology groups of "
			+ "a geometric object i'm interested in. It is intrinsic. "
			+ "So the design of this database structure had been inspired "
			+ "by the thoughts of geometry. Objects are named just "
			+ "based on some geometry concepts for my convenience, "
			+ "maybe not for others. "
			+ "\r\n "
			+ "\r\n" 
			+ "The side effect of this convenience is the "
			+ "impossible switching of concepts. I have no extra time "
			+ "to refactor these math concepts to CS counterparts. "
			+ "Actually, there is no monomorphism between the two fields. "
			+ "Inventing new words for CS is not a good ideal. "
			+ "Using the existing math definition is exact and convincing. "
			+ "\r\n "
			+ "\r\n" 
			+ "The basic storage structure is designed "
			+ "as a manifold, patched by pieces of "
			+ "data blocks, while it has little difference comparing "
			+ "to usual file system blocks, say, linux ext4. "
			+ "Consulting a geometry textbook will be helpful "
			+ "in understanding this. "
			+ "\r\n "
			+ "\r\n" 
			+ "So is the cache structure coming with the basic "
			+ "manifold. For example, the cache structure is "
			+ "a torus, which has local 2 dimensional structure. "
			+ "One is the free blocks in memory, the other is those " 
			+ "blocks that fail in competition and are waiting " 
			+ "to be released by a garbage collecting thread, " 
			+ "not the jvm one, but an internal cleansing thread "
			+ "of lunarbase. "
			+ "\r\n" 
			+ "\r\n" 
			+ "Though the concept term is hard to understand, " 
			+ "the logic is clear. And rest assured, the interfaces " 
			+ "exposed to end user are in computer scientific language, " 
			+ "and any programmer can apply it in practice. "
			+ "\r\n"
			+ "\r\n" 
			+ "Try its efficiency, the design is worth your time. "
			+ "\r\n"
			+ "\r\n"
			+ "Contact me at: feiben@lunarion.com "
			;
	
	

	static void printInfo()
	{
		System.out.println(readMe);
	}
	
	public static void main(String[] args) 
	{
		ReadMe.printInfo();
	}
}
