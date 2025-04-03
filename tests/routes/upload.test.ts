import { expect } from 'chai';
import request from 'supertest';
import app from '../../src/index'; 
import sinon from 'sinon';
import * as minioClient from '../../src/utils/minioClient'; 
describe('POST /upload', function () {

    let ensureBucketStub: sinon.SinonStub;
    let uploadToMinioStub: sinon.SinonStub;
  
    beforeEach(() => {
      ensureBucketStub = sinon.stub(minioClient, 'ensureBucketExists').resolves();
      
      uploadToMinioStub = sinon.stub(minioClient, 'uploadToMinio').resolves({
        success: true,
        message: 'Upload successful'
      });
    });
  
    afterEach(() => {
      sinon.restore();
    });
  

    it('should return 400 if no file is uploaded', async () => {
        const res = await request(app)
          .post('/upload')
          .query({ bucket: 'test-bucket' });
        
        expect(res.status).to.equal(400);
        expect(res.body.message).to.equal('No file uploaded');
      });

    //   it('should return 400 if no bucket is provided', async () => {
    //     const res = await request(app)
    //       .post('/upload')
    //       .attach('file', Buffer.from('test'), 'test.csv');
        
    //     expect(res.status).to.equal(400);
    //     expect(res.body.message).to.equal('No bucket provided');
    //   });
    //   it('should return 400 for invalid bucket names', async () => {
    //     const res = await request(app)
    //       .post('/upload')
    //       .query({ bucket: 'invalid bucket name!' })
    //       .attach('file', Buffer.from('test'), 'test.csv');
        
    //     expect(res.status).to.equal(400);
    //     expect(res.body.message).to.include('Bucket names must be between');
    //   });


    // it('should upload JSON successfully', async () => {
    //     const res = await request(app)
    //       .post('/upload')
    //       .query({ bucket: 'test-bucket', createBucketIfNotExists: 'true' })
    //       .attach('file', Buffer.from(JSON.stringify({test:1})), 'test.json');
        
    //     expect(res.status).to.equal(201);
    //     expect(res.body.status).to.equal('success');
    //     sinon.assert.calledWith(ensureBucketStub, 'test-bucket', true);
    //   });

    //   it('should handle bucket creation errors', async () => {
    //     ensureBucketStub.rejects(new Error('Bucket creation failed'));
    
    //     const res = await request(app)
    //       .post('/upload')
    //       .query({ bucket: 'problem-bucket', createBucketIfNotExists: 'true' })
    //       .attach('file', Buffer.from('test'), 'test.json');
        
    //     expect(res.status).to.equal(500);
    //     expect(res.body.status).to.equal('error');
    //   });
    
      
});
