import request from 'supertest';
import {
    expect
} from 'chai';
import app from '../../src/index';
import sinon, {
    SinonStub
} from 'sinon';

import * as climateService from '../../src/utils/minioClient';
import {
    downloadFileAndUpload
} from '../../src/utils/minioClient';
import logger from '../../src/logger';

describe('GET /download-climate-data', () => {
    let downloadStub: SinonStub;
    let loggerStub: SinonStub;

    beforeEach(() => {
        downloadStub = sinon.stub(climateService, 'downloadFileAndUpload');
        loggerStub = sinon.stub(logger, 'info');
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return success response when download and upload succeeds', async () => {
        downloadStub.resolves();

        const res = await request(app)
            .get('/download-climate-data?bucket=test-bucket')
            .expect(200);
        expect(res.body).to.deep.equal({
            download: 'success',
            upload: 'success'
        });
        expect(downloadStub.calledWith('test-bucket')).to.be.true;
        expect(loggerStub.calledWith('Downloading and uploading of climate data started')).to.be.true;
    });

    it('should return 500 when downloadFileAndUpload throws an error', async () => {
        const testError = new Error('Test error');
        downloadStub.rejects(testError);

        const res = await request(app)
            .get('/download-climate-data?bucket=test-bucket')
            .expect(500);

        expect(res.body).to.have.property('status', 'error');
        expect(res.body.message).to.equal('Test error');
    });
})