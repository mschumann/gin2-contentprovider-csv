package net.sf.iqser.plugin.csv.test;

import org.apache.log4j.Logger;

import com.iqser.core.analyzer.AnalyzerTaskStarter;
import com.iqser.core.analyzer.ContentAnalyzerJob;
import com.iqser.core.config.Configuration;
import com.iqser.core.exception.IQserTechnicalException;

/**
 * Supports testing of csv content provider.
 * 
 * @author Jšrg Wurzer
 *
 */
public class MockAnalyzerTaskStarter implements AnalyzerTaskStarter {
	
	private static Logger logger = Logger.getLogger( MockAnalyzerTaskStarter.class );

	public void startTask(ContentAnalyzerJob job) {
		logger.debug("startTask() startet for " + job.getContent().getContentUrl() + " and type " + job.getJobType());

		MockRepository rep = (MockRepository)Configuration.getConfiguration().getServiceLocator().getRepository();
		
		switch (job.getJobType()) {
		case (ContentAnalyzerJob.JOB_NEW): {
			try {
				rep.addContent(job.getContent());
			} catch (IQserTechnicalException e) {
				logger.error("Couldn't add Content "+ e.getMessage());
			}
			break;
		}
		case (ContentAnalyzerJob.JOB_CHANGE): {
			try {
				rep.updateContent(job.getContent());
			} catch (IQserTechnicalException e) {
				logger.error("Couldn't update Content "+ e.getMessage());
			}
			break;
		}
		case (ContentAnalyzerJob.JOB_DELETE): {
			try {
				rep.deleteContent(job.getContent());
			} catch (IQserTechnicalException e) {
				logger.error("Couldn't delete Content "+ e.getMessage());
			}
			break;
		}
		default:
			break;
		}
	}
}
